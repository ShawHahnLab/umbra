import queue
import threading
import time
import signal
import traceback
import csv
from .util import *
from . import project
from .box_uploader import BoxUploader
from .mailer import Mailer

class IlluminaProcessor:

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


    def __init__(self, path, config=None):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("IlluminaProcessor initializing...")
        if config is None:
            config = {}
        self.config = config
        self.path = Path(path).resolve(strict = True)
        paths = config.get("paths", {})
        self.path_runs = self._init_path(paths.get("runs", "runs"))
        self.path_exp = self._init_path(paths.get("experiments", "experiments"))
        self.path_status = self._init_path(paths.get("status", "status"))
        self.path_proc = self._init_path(paths.get("processed", "processed"))
        self.path_pack = self._init_path(paths.get("packaged", "packaged"))
        self.nthreads = config.get("nthreads", 1)
        self.nthreads_per_project = config.get("nthreads_per_project", 1)
        self.readonly = config.get("readonly")
        self._init_data()
        self._init_job_queue()
        self._init_completion_queue()
        # If a path to Box credentials was supplied, use a real uploader.
        # Otherwise just use a stub.
        config_box = config.get("box", {})
        path = config_box.get("credentials_path")
        if path and Path(path).exists():
            self.box = BoxUploader(path, config_box)
            self.uploader = self.box.upload
        else:
            msg = "No Box configuration given; skipping uploads."
            if self.readonly or config_box.get("skip"):
                self.logger.debug(msg)
            else:
                self.logger.warning(msg)
            self.uploader = lambda path: "https://"+str(path)
        # If mail-sending options were specified, use a real mailer.
        # Otherwise just use a stub.
        # Options can be listed as a separate credentials file or just inline.
        config_mail = config.get("mailer", {})
        path = config_mail.get("credentials_path")
        if path and Path(path).exists():
            creds = yaml_load(path)
            update_tree(config_mail, creds)
            self.mailerobj = Mailer(**config_mail)
            self.mailer = self.mailerobj.mail
        elif config_mail and not config_mail.get("skip"):
            self.mailerobj = Mailer(**config_mail)
            self.mailer = self.mailerobj.mail
        else:
            msg = "No Mailer configuration given; skipping emails."
            if self.readonly or config_mail.get("skip"):
                self.logger.debug(msg)
            else:
                self.logger.warning(msg)
            self.mailer = lambda *args, **kwargs: None
        self.logger.debug("IlluminaProcessor initialized.")

    def __del__(self):
        self.wait_for_jobs()

    def wait_for_jobs(self):
        """Wait for running jobs to finish."""
        self.logger.debug("wait_for_jobs started")
        readonly = getattr(self, "readonly", None)
        running = getattr(self, "running", None)
        q = getattr(self, "_queue_jobs", None)
        if q and running and not readonly:
            # join blocks until all tasks are done, as defined by task_done()
            # getting called.  So this will wait until everything placed on the
            # queue is both taken by a worker and finished processing.
            q.join()
        q = getattr(self, "_queue_completion", None)
        if q:
            self._update_queue_completion()
        self.logger.debug("wait_for_jobs completed")

    def load(self, wait=False):
        """Load all run and project data from scratch."""
        if self.running:
            self.wait_for_jobs()
        self._init_data()
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
        self.logger.debug("refresh started")
        # Refresh existing runs
        for run in self.runs:
            self.logger.debug("refresh run: %s" % run.run_id)
            run.refresh()
        # Load new runs, with callback _proc_new_alignment
        self._load_new_runs()
        # Consume finished projects from completion queue and record.
        # write report
        self._update_queue_completion()
        if wait:
            self.wait_for_jobs()
        self.logger.debug("refresh completed")

    def start(self):
        """Start the worker threads."""
        self.running = True
        for t in self._threads:
            try:
                t.start()
            except RuntimeError:
                pass

    def finish_up(self):
        """Stop refreshing data from disk.
        
        This will not interrupt jobs or stop processing from the queue, but
        will stop a watch_and_process loop."""
        self._queue_cmd.put("finish_up")

    def is_finishing_up(self):
        """Is a finish_up task in the command queue?
        
        This comes with all the caveats of checking the queue state without
        popping an item from it, but should be good enough for a quick
        check."""
        return("finish_up" in self._queue_cmd.queue)

    def watch_and_process(self, poll=5, wait=False):
        """Refresh continually, optionally waiting for completion each cycle.
        
        If a report was configured at intialization time, an updated report
        file will be generated each cycle as well."""
        # regularly refresh and process
        self.start()
        finish_up = False
        cmd = None
        self._queue_cmd = queue.Queue()
        signal.signal(signal.SIGINT,  self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGHUP,  self._signal_handler)
        signal.signal(signal.SIGUSR1, self._signal_handler)
        signal.signal(signal.SIGUSR2, self._signal_handler)
        self.logger.debug("starting processing loop")
        while not finish_up:
            self.logger.debug("starting process cycle")
            # The usual loop iteration is to refresh (unless we just did a full
            # reload on a previous cycle), update the report, and sleep for a
            # bit.
            if not cmd == "reload":
                self.logger.debug("refreshing")
                self.refresh(wait)
            report_args = self.config.get("save_report")
            if report_args:
                self.save_report(**report_args)
            time.sleep(poll)
            # Next any commands (such as caught from signals) are processed.
            try:
                while True:
                    self.logger.debug("reading commands")
                    cmd = self._queue_cmd.get_nowait()
                    if cmd == "reload":
                        self.logger.debug("cmd found: reloading")
                        self.load(wait)
                    elif cmd == "finish_up":
                        self.logger.debug("cmd found: finishing up")
                        finish_up = True
            except queue.Empty:
                self.logger.debug("done reading commands")
            self.logger.debug("finishing process cycle")
        self.logger.debug("exited processing loop")

    def create_report(self):
        """ Create a nested data structure summarizing processing status.
        
        This will be an ordered list of dictionaries, one per project and at
        least one per projectless alignment and run."""
        # Create lookup tables of Alignment to ProjectData and ProjectData to
        # processing Group first
        alignment_to_projects = {}
        project_to_group = {}
        for key in self.projects.keys():
            for proj in self.projects[key]:
                if not proj.alignment in alignment_to_projects.keys():
                    alignment_to_projects[proj.alignment] = []
                alignment_to_projects[proj.alignment].append(proj)
                project_to_group[proj] = key
        # Now, go through all Runs and Alignments, and fill in project information
        # where available.
        entries = []
        for run in self.runs:
            entry = {f: "" for f in IlluminaProcessor.REPORT_FIELDS}
            entry["RunId"] = run.run_id
            entry["RunPath"] = run.path
            if run.alignments:
                for idx, al in zip(range(len(run.alignments)), run.alignments):
                    entry_al = dict(entry)
                    entry_al["Alignment"] = idx
                    entry_al["Experiment"] = al.experiment
                    entry_al["AlignComplete"] = al.complete
                    projs = alignment_to_projects.get(al)
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
        entries.sort(key = lambda e: [e["RunId"], e["Alignment"], e["Project"]])
        return(entries)

    def report(self, out_file=sys.stdout, max_width=60):
        """ Render a CSV-formatted report to the given file handle."""
        entries = self.create_report()
        writer = csv.DictWriter(out_file, IlluminaProcessor.REPORT_FIELDS)
        writer.writeheader()
        for entry in entries:
            entry2 = entry
            for key in entry2:
                data = str(entry2[key])
                if max_width > 0 and len(data) > max_width:
                    data = data[0:(max_width-3)] + "..."
                entry2[key] = data
            writer.writerow(entry2)

    def save_report(self, path, max_width=60):
        """ Render a CSV-formatted report to the given file path."""
        mkparent(path)
        with open(path, "w") as f:
            self.report(f, max_width)

    ### Implementation details

    def _init_path(self, path):
        path = Path(path)
        if not path.is_absolute():
            path = self.path / path
        path = path.resolve()
        return(path)

    def _init_data(self):
        self.runs = set()
        self.projects = {
                "inactive":  set(),
                "active":    set(),
                "completed": set()
                }

    def _init_job_queue(self):
        # Set up the procesing threads and related info, but don't actually
        # start them yet.
        self.running = False
        self._queue_jobs = queue.Queue()
        self._threads = []
        if not self.readonly:
            for i in range(self.nthreads):
                t = threading.Thread(target=self._worker, daemon=True)
                self._threads.append(t)

    def _init_completion_queue(self):
        self._queue_completion = queue.Queue()

    def _load_new_runs(self):
        old_dirs = {run.path for run in self.runs}
        run_dirs = [Path(d).resolve() for d in self.path_runs.glob("*")]
        is_new_run_dir = lambda d: d.is_dir() and d not in old_dirs
        run_dirs = [d for d in run_dirs if is_new_run_dir(d)]
        runs = {self._run_setup(run_dir) for run_dir in run_dirs}
        runs = {run for run in runs if run}
        self.runs |= runs

    def _run_setup(self, run_dir):
        run = None
        # The min and max run directory ctime age in seconds, time of last
        # change of run directory, and current time.
        min_age = self.config.get("min_age")
        max_age = self.config.get("max_age")
        time_change = run_dir.stat().st_ctime
        time_now = time.time()
        # Now, check each threshold if it was specified.  Careful to check for
        # None here because a literal zero should be taken as its own meaning.
        if min_age is not None and (time_now - time_change < min_age):
            self.logger.debug("skipping run; timestamp too new:.../%s" % run_dir.name)
            return(run)
        if max_age is not None and (time_now - time_change > max_age):
            self.logger.debug("skipping run; timestamp too old:.../%s" % run_dir.name)
            return(run)
        try:
            self.logger.debug("loading new run:.../%s" % run_dir.name)
            run = illumina.run.Run(run_dir,
                    strict = True,
                    alignment_callback = self._proc_new_alignment,
                    min_alignment_dir_age = min_age)
        except Exception as e:
            # ValueError for unrecognized or mismatched directories
            if type(e) is ValueError:
                self.logger.debug("skipped unrecognized run: %s" % run_dir)
            else:
                self.logger.critical("Error while loading run %s" % run_dir)
                raise e
        return(run)

    def _update_queue_completion(self):
        try:
            while True:
                proj = self._queue_completion.get_nowait()
                self.logger.debug("Filing project in completed set: \"%s\"" %
                        proj.work_dir)
                self.projects["active"].remove(proj)
                self.projects["completed"].add(proj)
        except queue.Empty:
            pass

    def _signal_handler(self, sig, frame):
        """Receive and process OS signals.
        
        This will just toggle variables to inform watch_and_process of what it
        needs to do next.  But, if two INT/TERM signals are sent in a row, the
        program will exit without waiting."""
        if sig in [signal.SIGINT, signal.SIGTERM]:
            if not self.is_finishing_up():
                msg = "Signal caught (%s), finishing up" % sig
                self.logger.warning(msg)
                self.finish_up()
            else:
                msg = "Second signal caught (%s), stopping now" % sig
                self.logger.error(msg)
                sys.exit(1)
        elif sig in [signal.SIGHUP]:
            msg = "Signal caught (%s),"
            msg += " re-loading all data after current tasks finish."
            msg = msg % sig
            self.logger.warning(msg)
            #self._reload = True
            self._queue_cmd.put("reload")
        elif sig in [signal.SIGUSR1]:
            msg = "Signal caught (%s),"
            msg += " decreasing loglevel (increasing verbosity)."
            msg = msg % sig
            self._loglevel_shift(-10)
            self.logger.warning(msg)
        elif sig in [signal.SIGUSR2]:
            msg = "Signal caught (%s),"
            msg += " increasing loglevel (decreasing verbosity)."
            msg = msg % sig
            self._loglevel_shift(10)
            self.logger.warning(msg)

    def _loglevel_shift(self, step):
        """Change the root logger's level by a relative amount.
        
        Positive numbers give less verbose logging.   Steps of ten match
        Python's named levels.  A minimum of zero is applied."""
        logger = logging.getLogger()
        lvl_current = logger.getEffectiveLevel()
        lvl_new = max(0, lvl_current + step)
        self.logger.warning("Changing loglevel: %d -> %d" %
                (lvl_current, lvl_new))
        logger.setLevel(lvl_new)

    def _proc_new_alignment(self, al):
        """Match a newly-completed Alignment to Project information.
        
        Any new projects will be marked active and enqueued for processing.
        Any projects with previously-created metadata on disk will be marked
        inactive and not processed."""
        self.logger.debug("proccesing new alignment: %s" % al.path)
        projs = project.ProjectData.from_alignment(
                alignment = al,
                path_exp = self.path_exp,
                dp_align = self.path_status,
                dp_proc = self.path_proc,
                dp_pack = self.path_pack,
                uploader = self.uploader,
                mailer = self.mailer,
                nthreads = self.nthreads_per_project,
                readonly = self.readonly)
        for proj in projs:
            suffix = ""
            if proj.readonly or proj.status == project.ProjectData.FAILED:
                grp = "inactive"
                self.projects[grp].add(proj)
                if proj.status != project.ProjectData.COMPLETE:
                    suffix = " (Incomplete: %s)" % proj.status
            else:
                grp = "active"
                self.projects[grp].add(proj)
                self._queue_jobs.put(proj)
            msg = "found new project [%s] : %s%s" % (grp, proj.work_dir, suffix)
            self.logger.info(msg)

    def _worker(self):
        """Pull ProjectData objects from job queue and process.
        
        This function is intended for use in separate worker threads, so any
        exceptions raised during project data processing are logged but not
        re-raised."""
        while True:
            proj = self._queue_jobs.get()
            try:
                proj.process()
            except Exception as e:
                subject = "Failed project: %s" % proj.name
                self.logger.error(subject)
                self.logger.error(traceback.format_exc())
                config_mail = self.config.get("mailer", {})
                contacts = config_mail.get("to_addrs_on_error") or []
                body = "Project processing failed for \"%s\"" % proj.work_dir
                body += " with the following message:\n"
                body += "\n\n"
                body += traceback.format_exc()
                kwargs = {
                        "to_addrs": contacts,
                        "subject": subject,
                        "msg_body": body
                        }
                self.mailer(**kwargs)
            finally:
                self.logger.debug("Declaring project done: \"%s\"" %
                        proj.work_dir)
                self._queue_jobs.task_done()
                self.logger.debug("Placing project on completion queue: \"%s\"" %
                        proj.work_dir)
                self._queue_completion.put(proj)
