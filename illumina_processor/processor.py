import queue
import threading
import time
import signal
import traceback
from .util import *
from . import project
from .box_uploader import BoxUploader
from .mailer import Mailer

class IlluminaProcessor:

    def __init__(self, path, config=None):
        self.logger = logging.getLogger(__name__)
        self.logger.debug("IlluminaProcessor initializing...")
        if config is None:
            config = {}
        self.path = Path(path).resolve(strict = True)
        paths = config.get("paths", {})
        self.path_runs = self._init_path(paths.get("runs", "runs"))
        self.path_exp = self._init_path(paths.get("experiments", "experiments"))
        self.path_status = self._init_path(paths.get("status", "status"))
        self.path_proc = self._init_path(paths.get("processed", "processed"))
        self.path_pack = self._init_path(paths.get("packaged", "packaged"))
        self.path_report = self._init_path(paths.get("report", "report.csv"))
        self.nthreads = config.get("nthreads", 1)
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
            if config_box.get("skip"):
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
            self.mailerobj = Mailer(**creds)
            self.mailer = self.mailerobj.mail
        elif config_mail and not config_mail.get("skip"):
            self.mailerobj = Mailer(**config_mail)
            self.mailer = self.mailerobj.mail
        else:
            msg = "No Mailer configuration given; skipping emails."
            if config_mail.get("skip"):
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
        q = getattr(self, "_queue_jobs", None)
        if q:
            q.join()
        self._update_queue_completion()
        self.logger.debug("wait_for_jobs completed")

    def load(self, wait=False):
        """Load all run and project data from scratch."""
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

    def watch_and_process(self, poll=5):
        # regularly refresh and process
        # catch signals:
        # exiting the loop: SIGINT/QUIT/KeyboardInterrupt
        # calling load(): USR1
        # debugging?: USR2
        self._finish_up = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        while not self._finish_up:
            self.refresh()
            time.sleep(poll)

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
        # Start up processing threads. They'll block when the queue is empty,
        # so to start with they'll all just be waiting for jobs to do.
        self._queue_jobs = queue.Queue()
        self._threads = []
        for i in range(self.nthreads):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self._threads.append(t)

    def _init_completion_queue(self):
        self._queue_completion = queue.Queue()

    def _load_new_runs(self):
        old_dirs = {run.path for run in self.runs}
        run_dirs = [Path(d).resolve() for d in self.path_runs.glob("*")]
        is_new_run_dir = lambda d: d.is_dir() and d not in old_dirs
        run_dirs = [d for d in run_dirs if is_new_run_dir(d)]
        runs = {self._run_setup(run_dir) for run_dir in run_dirs}
        self.runs |= runs

    def _run_setup(self, run_dir):
        try:
            run = illumina.run.Run(run_dir,
                    strict = True,
                    alignment_callback = self._proc_new_alignment)
            self.logger.debug("loaded new run: %s" % run.run_id)
        except Exception as e:
            # ValueError for unrecognized or mismatched directories
            if type(e) is ValueError:
                run = None
                self.logger.debug("skipped unrecognized run: %s" % run_dir)
            else:
                self.logger.critical("Error while loading run %s\n" % run_dir)
                raise e
        return(run)

    def _update_queue_completion(self):
        try:
            while True:
                proj = self._queue_completion.get_nowait()
                self.projects["active"].remove(proj)
                self.projects["completed"].add(proj)
        except queue.Empty:
            pass

    def _signal_handler(self, sig, frame):
        if not self._finish_up:
            msg = "Signal caught (%s), finishing up" % sig
            self.logger.warning(msg)
            self._finish_up = True
        else:
            msg = "Second signal caught (%s), stopping now" % sig
            self.logger.error(msg)
            sys.exit(1)

    def _proc_new_alignment(self, al):
        """Match a newly-completed Alignment to Project information.
        
        Any new projects will be marked active and enqueued for processing.
        Any projects with previously-created metadata on disk will be marked
        inactive and not processed."""
        self.logger.debug("proccesing new alignment: %s" % al.path)
        projs = project.ProjectData.from_alignment(al,
                self.path_exp,
                self.path_status,
                self.path_proc,
                self.path_pack,
                self.uploader,
                self.mailer)
        for proj in projs:
            suffix = ""
            if proj.readonly:
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
                self.logger.error("Failed project: %s\n" % proj.name)
                self.logger.error(traceback.format_exc())
            self._queue_jobs.task_done()
            self._queue_completion.put(proj)
