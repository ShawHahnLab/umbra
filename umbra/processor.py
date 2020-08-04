"""
Manage a directory of Illumina run data and coordinate processing.

See IlluminaProcessor class for usage.
"""

import sys
import queue
import threading
import time
import signal
import traceback
import logging
import csv
import copy
from pathlib import Path
from . import CONFIG
from . import project
from .box_uploader import BoxUploader
from .mailer import Mailer
from .config import update_tree
from .illumina.run import Run
from .util import yaml_load, mkparent

LOGGER = logging.getLogger(__name__)

class IlluminaProcessor:
    """
    Manage a directory of Illumina run data and coordinate processing.

    This will regularly scan a directory for new Illumina run data and execute
    processing tasks, as defined in metadata spreadsheets, in separate worker
    threads.  Logging and signal handling are configured to allow this class to
    act as the main interface for a long-running OS service.
    """

    REPORT_FIELDS = [
                # Run attributes
                "RunId",         # Illumina Run ID
                "RunPath",       # Directory path
                # Alignment attributes
                "Alignment",     # Alignment number in current set
                "Experiment",    # Name of experiment from sample sheet
                "AlignComplete", # Is the alignment complete?
                # Project attributes
                "Project",       # Name of project from extra metadata
                "WorkDir",       # Project working directory name
                "Status",        # Project data processing status
                "NSamples",      # Num samples in project data
                "NFiles",        # Num files in project data
                # Processor attributes
                "Group"]


    def __init__(self, path, conf=None):
        LOGGER.debug("IlluminaProcessor initializing...")
        self.conf = copy.deepcopy(CONFIG)
        update_tree(self.conf, conf or {})
        self.path = Path(path).resolve(strict=True)
        paths = conf.get("paths", {})
        self.paths = {
            "runs": self._init_path(paths.get("runs", "runs")),
            "exp": self._init_path(paths.get("experiments", "experiments")),
            "status": self._init_path(paths.get("status", "status")),
            "proc": self._init_path(paths.get("processed", "processed")),
            "pack": self._init_path(paths.get("packaged", "packaged"))
            }

        # seqinfo dict references run and project information
        self.seqinfo = self._init_seqinfo()
        # procstatus dict contains info on processing threads and queues
        self.procstatus = self._init_procstatus()
        # If a path to Box credentials was supplied, use a real uploader.
        # Otherwise just use a stub.
        conf_box = conf.get("box", {})
        path = conf_box.get("credentials_path")
        if not conf_box.get("skip") and path and Path(path).exists():
            self.box = BoxUploader(path, conf_box)
        else:
            msg = "No Box configuration given; skipping uploads."
            if self.readonly or conf_box.get("skip"):
                LOGGER.debug(msg)
            else:
                LOGGER.warning(msg)
            self.box = lambda: None
            self.box.upload = lambda path: "https://"+str(path)
        # If mail-sending options were specified, use a real mailer.
        # Otherwise just use a stub.
        # Options can be listed as a separate credentials file or just inline.
        conf_mail = conf.get("mailer", {})
        path = conf_mail.get("credentials_path")
        if path and Path(path).exists():
            creds = yaml_load(path)
            update_tree(conf_mail, creds)
            self.mailerobj = Mailer(conf_mail)
        elif conf_mail and not conf_mail.get("skip"):
            self.mailerobj = Mailer(conf_mail)
        else:
            msg = "No Mailer configuration given; skipping emails."
            if self.readonly or conf_mail.get("skip"):
                LOGGER.debug(msg)
            else:
                LOGGER.warning(msg)
            self.mailerobj = lambda: None
            self.mailerobj.mail = lambda *args, **kwargs: None
        LOGGER.debug("IlluminaProcessor initialized.")

    def __del__(self):
        self.wait_for_jobs()

    @property
    def readonly(self):
        """Is the processor configured to view only and not process?"""
        return self.conf.get("readonly", False)

    @property
    def running(self):
        """Is the processing currently running?"""
        return getattr(self, "procstatus", {}).get("running")

    def wait_for_jobs(self):
        """Wait for running jobs to finish."""
        LOGGER.debug("wait_for_jobs started")
        pstat = getattr(self, "procstatus", {})
        qjobs = pstat.get("queue_jobs")
        if qjobs and self.running and not self.readonly:
            # join blocks until all tasks are done, as defined by task_done()
            # getting called.  So this will wait until everything placed on the
            # queue is both taken by a worker and finished processing.
            qjobs.join()
        if pstat.get("queue_completion"):
            self._update_queue_completion()
        LOGGER.debug("wait_for_jobs completed")

    def load(self, wait=False):
        """Load all run and project data from scratch."""
        if self.running:
            self.wait_for_jobs()
        self.seqinfo = self._init_seqinfo()
        self.refresh(wait)

    def refresh(self, wait=False):
        """Find new Run data.

        This will load new Run directories, load new Alignments and check
        completion for existing Runs, and load completed Alignments for
        previously-incomplete Alignments.  New entries found are merged with
        existing ones.  This only loads data very selectively from disk; if any
        other files may have changed, call load instead.

        If wait is True, wait_for_jobs() will be called at the end so that any
        newly-scheduled jobs complete before the function returns."""
        LOGGER.debug("refresh started")
        # Refresh existing runs
        for run in self.seqinfo["runs"]:
            LOGGER.debug("refresh run: %s", run.run_id)
            run.refresh()
        # Load new runs, with callback _proc_new_alignment
        self._load_new_runs()
        # Consume finished projects from completion queue and record.
        # write report
        self._update_queue_completion()
        if wait:
            self.wait_for_jobs()
        LOGGER.debug("refresh completed")

    def start(self):
        """Start the worker threads."""
        self.procstatus["running"] = True
        for thread in self.procstatus["threads"]:
            try:
                thread.start()
            except RuntimeError:
                pass

    def finish_up(self):
        """Stop refreshing data from disk.

        This will not interrupt jobs or stop processing from the queue, but
        will stop a watch_and_process loop."""
        self.procstatus["queue_cmd"].put("finish_up")

    def is_finishing_up(self):
        """Is a finish_up task in the command queue?

        This comes with all the caveats of checking the queue state without
        popping an item from it, but should be good enough for a quick
        check."""
        return "finish_up" in self.procstatus["queue_cmd"].queue

    def watch_and_process(self, poll=5, wait=False):
        """Refresh continually, optionally waiting for completion each cycle.

        If a report was configured at intialization time, an updated report
        file will be generated each cycle as well."""
        # regularly refresh and process
        self.start()
        finish_up = False
        cmd = None
        signal.signal(signal.SIGINT, self._cb_signal_handler)
        signal.signal(signal.SIGTERM, self._cb_signal_handler)
        signal.signal(signal.SIGHUP, self._cb_signal_handler)
        signal.signal(signal.SIGUSR1, self._cb_signal_handler)
        signal.signal(signal.SIGUSR2, self._cb_signal_handler)
        LOGGER.debug("starting processing loop")
        while not finish_up:
            LOGGER.debug("starting process cycle")
            # The usual loop iteration is to refresh (unless we just did a full
            # reload on a previous cycle), update the report, and sleep for a
            # bit.
            if not cmd == "reload":
                LOGGER.debug("refreshing")
                self.refresh(wait)
            report_args = self.conf.get("save_report")
            if report_args:
                self.save_report(**report_args)
            time.sleep(poll)
            # Next any commands (such as caught from signals) are processed.
            try:
                while True:
                    LOGGER.debug("reading commands")
                    cmd = self.procstatus["queue_cmd"].get_nowait()
                    if cmd == "reload":
                        LOGGER.debug("cmd found: reloading")
                        self.load(wait)
                    elif cmd == "finish_up":
                        LOGGER.debug("cmd found: finishing up")
                        finish_up = True
            except queue.Empty:
                LOGGER.debug("done reading commands")
            LOGGER.debug("finishing process cycle")
        LOGGER.debug("exited processing loop")

    def create_report(self):
        """ Create a nested data structure summarizing processing status.

        This will be an ordered list of dictionaries, one per project and at
        least one per projectless alignment and run."""
        # Create lookup tables of Alignment to ProjectData and ProjectData to
        # processing Group first
        alignment_to_projects = {}
        project_to_group = {}
        for key in self.seqinfo["projects"]:
            for proj in self.seqinfo["projects"][key]:
                if not proj.alignment in alignment_to_projects.keys():
                    alignment_to_projects[proj.alignment] = []
                alignment_to_projects[proj.alignment].append(proj)
                project_to_group[proj] = key
        # Now, go through all Runs and Alignments, and fill in project information
        # where available.
        entries = []
        for run in self.seqinfo["runs"]:
            entry = {f: "" for f in IlluminaProcessor.REPORT_FIELDS}
            entry["RunId"] = run.run_id
            entry["RunPath"] = run.path
            if run.alignments:
                for idx, aln in zip(range(len(run.alignments)), run.alignments):
                    entry_al = dict(entry)
                    entry_al["Alignment"] = idx
                    entry_al["Experiment"] = aln.experiment
                    entry_al["AlignComplete"] = aln.complete
                    projs = alignment_to_projects.get(aln)
                    if projs:
                        for proj in projs:
                            entry_proj = dict(entry_al)
                            entry_proj["Project"] = proj.name
                            entry_proj["WorkDir"] = proj.work_dir
                            entry_proj["Status"] = proj.status
                            entry_proj["NSamples"] = len(proj.experiment_info["sample_names"])
                            entry_proj["NFiles"] = sum([len(x) for x in proj.sample_paths.values()])
                            entry_proj["Group"] = project_to_group[proj]
                            entries.append(entry_proj)
                    else:
                        entries.append(entry_al)
            else:
                entries.append(entry)
        entries.sort(key=lambda e: [e["RunId"], e["Alignment"], e["Project"]])
        return entries

    def report(self, out_file=sys.stdout, max_width=60):
        """ Render a CSV-formatted report to the given file handle.

        max_width: maximum column width in characters.  Strings beyond this
        length will be truncated and displayed with "..."  Set to 0 for no
        maximum."""
        entries = self.create_report()
        writer = csv.DictWriter(out_file, IlluminaProcessor.REPORT_FIELDS)
        writer.writeheader()
        for entry in entries:
            entry2 = entry
            for key in entry2:
                data = str(entry2[key])
                if 0 < max_width < len(data):
                    data = data[0:(max_width-3)] + "..."
                entry2[key] = data
            writer.writerow(entry2)

    def save_report(self, path, max_width=60):
        """ Render a CSV-formatted report to the given file path.

        max_width: maximum column width in characters.  Strings beyond this
        length will be truncated and displayed with "..."  Set to 0 for no
        maximum."""
        mkparent(path)
        with open(path, "w") as fout:
            self.report(fout, max_width)

    ### Implementation details

    def _init_path(self, path):
        path = Path(path)
        if not path.is_absolute():
            path = self.path / path
        path = path.resolve()
        return path

    @staticmethod
    def _init_seqinfo():
        seqinfo = {
            "runs": set(),
            "projects": {
                "inactive":  set(),
                "active":    set(),
                "completed": set()
                }
            }
        return seqinfo

    def _init_procstatus(self):
        # Set up the procesing threads and related info, but don't actually
        # start them yet.
        procstatus = {}
        procstatus["running"] = False
        procstatus["queue_jobs"] = queue.Queue()
        procstatus["threads"] = []
        if not self.readonly:
            for i in range(self.conf["nthreads"]):
                LOGGER.debug("Starting thread %d", i)
                thread = threading.Thread(target=self._worker, daemon=True)
                procstatus["threads"].append(thread)
        LOGGER.debug("Initialized job queue and worker threads")
        procstatus["queue_completion"] = queue.Queue()
        procstatus["queue_cmd"] = queue.Queue()
        return procstatus

    def _load_new_runs(self):
        old_dirs = {run.path for run in self.seqinfo["runs"]}
        run_dirs = [Path(d).resolve() for d in self.paths["runs"].glob("*")]
        is_new_run_dir = lambda d: d.is_dir() and d not in old_dirs
        run_dirs = [d for d in run_dirs if is_new_run_dir(d)]
        runs = {self._run_setup(run_dir) for run_dir in run_dirs}
        runs = {run for run in runs if run}
        self.seqinfo["runs"] |= runs

    def _run_setup(self, run_dir):
        run = None
        # The min and max run directory ctime age in seconds, time of last
        # change of run directory, and current time.
        min_age = self.conf.get("min_age")
        max_age = self.conf.get("max_age")
        time_change = run_dir.stat().st_ctime
        time_now = time.time()
        # Now, check each threshold if it was specified.  Careful to check for
        # None here because a literal zero should be taken as its own meaning.
        if min_age is not None and (time_now - time_change < min_age):
            LOGGER.info("skipping run; timestamp too new:.../%s", run_dir.name)
            return run
        if max_age is not None and (time_now - time_change > max_age):
            LOGGER.info("skipping run; timestamp too old:.../%s", run_dir.name)
            return run
        # pylint: disable=broad-except
        try:
            LOGGER.debug("loading new run:.../%s", run_dir.name)
            run = Run(
                run_dir,
                strict=True,
                alignment_callback=self._proc_new_alignment,
                min_alignment_dir_age=min_age)
        except Exception as exception:
            # ValueError for unrecognized or mismatched directories
            if isinstance(exception, ValueError):
                LOGGER.debug("skipped unrecognized run: %s", run_dir)
            else:
                LOGGER.critical("Error while loading run %s", run_dir)
                raise exception
        return run

    def _update_queue_completion(self):
        try:
            while True:
                proj = self.procstatus["queue_completion"].get_nowait()
                LOGGER.debug(
                    "Filing project in completed set: \"%s\"", proj.work_dir)
                self.seqinfo["projects"]["active"].remove(proj)
                self.seqinfo["projects"]["completed"].add(proj)
        except queue.Empty:
            pass

    def _cb_signal_handler(self, sig, frame):
        """Receive and process OS signals.

        This will just toggle variables to inform watch_and_process of what it
        needs to do next.  But, if two INT/TERM signals are sent in a row, the
        program will exit without waiting."""
        if sig in [signal.SIGINT, signal.SIGTERM]:
            if not self.is_finishing_up():
                LOGGER.warning(
                    "Signal caught (%s), finishing up", sig)
                self.finish_up()
            else:
                LOGGER.error(
                    "Second signal caught (%s), stopping now", sig)
                sys.exit(1)
        elif sig in [signal.SIGHUP]:
            msg = "Signal caught (%s),"
            msg += " re-loading all data after current tasks finish."
            LOGGER.warning(msg, sig)
            #self._reload = True
            self.procstatus["queue_cmd"].put("reload")
        elif sig in [signal.SIGUSR1]:
            msg = "Signal caught (%s),"
            msg += " decreasing loglevel (increasing verbosity)."
            _loglevel_shift(-10)
            LOGGER.warning(msg, sig)
        elif sig in [signal.SIGUSR2]:
            msg = "Signal caught (%s),"
            msg += " increasing loglevel (decreasing verbosity)."
            _loglevel_shift(10)
            LOGGER.warning(msg, sig)

    def _proc_new_alignment(self, aln):
        """Match a newly-completed Alignment to Project information.

        Any new projects will be marked active and enqueued for processing.
        Any projects with previously-created metadata on disk will be marked
        inactive and not processed."""
        LOGGER.debug("proccesing new alignment: %s", aln.path)
        projs = project.ProjectData.from_alignment(
            alignment=aln,
            path_exp=self.paths["exp"],
            dp_align=self.paths["status"],
            dp_proc=self.paths["proc"],
            dp_pack=self.paths["pack"],
            uploader=self.box.upload,
            mailer=self.mailerobj.mail,
            nthreads=self.conf["nthreads_per_project"],
            readonly=self.readonly,
            conf=self.conf.get("task_options", {}))
        for proj in projs:
            suffix = ""
            if proj.readonly or proj.status == project.ProjectData.FAILED:
                grp = "inactive"
                self.seqinfo["projects"][grp].add(proj)
                if proj.status != project.ProjectData.COMPLETE:
                    suffix = " (Incomplete: %s)" % proj.status
            else:
                grp = "active"
                self.seqinfo["projects"][grp].add(proj)
                self.procstatus["queue_jobs"].put(proj)
            msg = "found new project [%s] : %s%s" % (grp, proj.work_dir, suffix)
            LOGGER.info(msg)

    def _worker(self):
        """Pull ProjectData objects from job queue and process.

        This function is intended for use in separate worker threads, so any
        exceptions raised during project data processing are logged but not
        re-raised."""
        # pylint: disable=broad-except
        while True:
            proj = self.procstatus["queue_jobs"].get()
            try:
                proj.process()
            except Exception:
                subject = "Failed project: %s" % proj.name
                LOGGER.error(subject)
                LOGGER.error(traceback.format_exc())
                conf_mail = self.conf.get("mailer", {})
                contacts = conf_mail.get("to_addrs_on_error") or []
                body = "Project processing failed for \"%s\"" % proj.work_dir
                body += " with the following message:\n"
                body += "\n\n"
                body += traceback.format_exc()
                kwargs = {
                    "to_addrs": contacts,
                    "subject": subject,
                    "msg_body": body
                    }
                self.mailerobj.mail(**kwargs)
            finally:
                LOGGER.debug(
                    "Declaring project done: \"%s\"", proj.work_dir)
                self.procstatus["queue_jobs"].task_done()
                LOGGER.debug(
                    "Placing project on completion queue: \"%s\"", proj.work_dir)
                self.procstatus["queue_completion"].put(proj)


def _loglevel_shift(step):
    """Change the root logger's level by a relative amount.

    Positive numbers give less verbose logging.   Steps of ten match
    Python's named levels.  A minimum of zero is applied."""
    lvl_current = LOGGER.getEffectiveLevel()
    lvl_new = max(0, lvl_current + step)
    LOGGER.warning(
        "Changing loglevel: %d -> %d", lvl_current, lvl_new)
    LOGGER.setLevel(lvl_new)
