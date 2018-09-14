import csv
import warnings
import queue
import threading
import time
import signal

from .util import *
from .project import *
from .box_uploader import BoxUploader
from .illumina import run

class IlluminaProcessor:
    """Manage processing for incoming Illumina runs.
    
    This class tracks Illumina runs and associated project data, schedules
    processing in parallel, and packages finished project data directories."""

    def __init__(self, path_runs, path_exp, path_align):
        self.path_runs  = Path(path_runs)
        self.path_exp   = Path(path_exp)
        self.path_align = Path(path_align)
        self.runs = []
        self._setup_queue()

    def __del__(self):
        self.wait_for_jobs()

    def load_run_data(self):
        """Match up Run directories with per-experiment data, from scratch."""
        self.runs = [] # dump existing data
        self.refresh() # load from scratch

    def refresh(self):
        """Find new Run data.
        
        This will load new Run directories, load new Alignments and check
        completion for existing Runs, and load completed Alignments for
        previously-incomplete Alignments.  New entries found are merged with
        existing ones.  This only loads data very selectively from disk; if any
        other files may have changed, call load_run_data instead."""
        # Process existing runs for RTA compltion, alignment completion, and
        # new alignments.
        for run in self.runs:
            run.refresh()
        # Find all run directories not already known
        run_dirs_last = [run.path for run in self.runs]
        run_dirs = [Path(d).resolve() for d in self.path_runs.glob("*") if d.is_dir()]
        is_new = lambda d: not d in run_dirs_last
        run_dirs = [d for d in run_dirs if is_new(d)]
        runs = [self._run_setup_with_checks(run_dir) for run_dir in run_dirs]
        # Ignore unrecognized (None) entries
        runs = [run for run in runs if run]
        for run in runs:
            for al in run.alignments:
                self._match_alignment_to_projects(al)
        self.runs += runs

    def _run_setup_with_checks(self, run_dir):
        """Create a Run object for the given path, or None if no valid run is found.
        
        A valid Run here must have a directory name matching the Run ID inside
        the run directory and throw no ValueError during initialization."""
        try:
            run = illumina.run.Run(run_dir, strict = True)
        except Exception as e:
            # ValueError for unrecognized or mismatched directories
            if type(e) is ValueError:
                run = None
            else:
                sys.stderr.write("Error while loading run %s\n" % run_dir)
                raise e
        return(run)

    def _match_alignment_to_projects(self, al):
        """Add Project information to an Alignment."""

        al.projects = project.ProjectData.from_alignment(al, self.path_exp,
                self.path_align)
        if al.projects:
            # projects not marked complete
            is_complete = lambda k: al.projects[k].status != project.ProjectData.COMPLETE
            incompletes = [al.projects[k] for k in al.projects if is_complete(k)]
            # Are any of the projects for this run+alignment not yet complete?
            if incompletes:
                # And, is the alignment itself complete?  If not just skip all
                # the FASTQ-handling parts here.  If we're missing files,
                # complain and then just proceed with none loaded.
                sample_paths = None
                if al.complete:
                    try:
                        sample_paths = al.sample_paths()
                    except FileNotFoundError as e:
                        msg = "\nFASTQ file not found:\n"
                        msg += "Run:       %s\n" % al.run.path
                        msg += "Alignment: %s\n" % al.path
                        msg += "File:      %s\n" % e.filename
                        warnings.warn(msg)
                for proj_key in al.projects:
                    proj = al.projects[proj_key]
                    #proj.load_metadata(dp_align = self.path_align)
                    proj.set_sample_paths(sample_paths)

    def watch_and_process(self, poll=5):
        """Regularly check for new data and enqueue projects for processing."""
        self._finish_up = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        while not self._finish_up:
            self.refresh()
            self.process()
            sys.stderr.write("Projects in queue: %d\n" % self._queue.qsize())
            time.sleep(poll)

    def _signal_handler(self, sig, frame):
        if not self._finish_up:
            sys.stderr.write("Signal caught (%s), finishing up\n" % sig)
            self._finish_up = True
        else:
            sys.stderr.write("Second signal caught (%s), stopping now\n" % sig)
            sys.exit(1)

    def process(self):
        """Update queue of unfinished projects."""
        # For every project of every alignment of every run, *if* a project is
        # incomplete, but its alignment is complete, add the project to the
        # queue.
        for run in self.runs:
            for al in run.alignments:
                if al.complete and al.projects:
                    for key in al.projects:
                        proj = al.projects[key]
                        if proj.status != project.ProjectData.COMPLETE:
                            self.process_project(proj)

    def wait_for_jobs(self):
        """Wait for running jobs to finish."""
        q = getattr(self, "_queue")
        if q:
            q.join()

    def process_project(self, proj):
        """Add project to queue if not already present."""
        if not proj in self._projects_enqueued:
            sys.stderr.write("Enqueue project: %s\n" % proj.name)
            self._projects_enqueued.add(proj)
            self._queue.put(proj)

    def _setup_queue(self):
        # Start up processing threads. They'll block when the queue is empty,
        # so to start with they'll all just be waiting for jobs to do.
        self._queue = queue.Queue()
        self._projects_enqueued = set()
        nthreads = 4
        self.threads = []
        for i in range(nthreads):
            t = threading.Thread(target=self._processor, daemon=True)
            t.start()
            self.threads.append(t)

    def _processor(self):
        """Handler for individual ProjectData processing."""
        while True:
            proj = self._queue.get()
            sys.stderr.write("Process project: %s\n" % proj.name)
            proj.process()
            self._queue.task_done()
