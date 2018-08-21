import csv
import warnings
import queue
import threading
import time
import signal

from util import *
from box_uploader import BoxUploader
import illumina.run
import experiment
import project

PATH_ROOT  = Path(__file__).parent / "testdata"
PATH_RUNS  = PATH_ROOT / "runs"
PATH_EXP   = PATH_ROOT / "experiments"
PATH_ALIGN = PATH_ROOT / "alignments"
PATH_PROC  = PATH_ROOT / "processed"
PATH_PACK  = PATH_ROOT / "packaged"

class IlluminaProcessor:

    def __init__(self, path_runs, path_exp, path_align):
        self.path_runs  = Path(path_runs)
        self.path_exp   = Path(path_exp)
        self.path_align = Path(path_align)
        self._setup_queue()


    def match_alignment_to_projects(self, al):
        """Add Experiment and Project information to an Alignment."""

        al.experiment_info = None
        al.projects = None
        # TODO try loading the associated sample sheet too.  If it can't be
        # found or if it doesn't match the previous sample sheet, throw a
        # warning.
        exp_path = self.path_exp / al.experiment / "metadata.csv"
        al.experiment_path = exp_path
        try:
            # Load the spreadsheet of per-sample project information
            al.experiment_info = experiment.load_metadata(exp_path)
        except FileNotFoundError:
            pass
        else:
            # If that was found, do some extra processing to link up sample and
            # project data.
            al.projects = project.ProjectData.from_alignment(al)
            # projects not marked complete
            is_complete = lambda k: al.projects[k].status != project.ProjectData.COMPLETE
            incompletes = [al.projects[k] for k in al.projects if is_complete(k)]
            # Are any of the projects for this run+alignmet not yet complete?
            if incompletes:
                # And, is the alignment itself complete?  If not just skip all
                # the FASTQ-handling parts here.  If we're missing files,
                # complain and then just proceed with none loaded.
                sample_paths = None
                if al.complete:
                    try:
                        sample_paths = al.sample_paths_by_name()
                    except FileNotFoundError as e:
                        msg = "\nFASTQ file not found:\n"
                        msg += "Run:       %s\n" % al.run.path
                        msg += "Alignment: %s\n" % al.path
                        msg += "File:      %s\n" % e.filename
                        warnings.warn(msg)
                for proj_key in al.projects:
                    proj = al.projects[proj_key]
                    proj.load_metadata(dp_align = self.path_align)
                    proj.set_sample_paths(sample_paths)

    def run_setup_with_checks(self, run_dir):
        """Create a Run object for the given path, or None if no run is found."""
        try:
            run = illumina.run.Run(run_dir)
        except Exception as e:
            # ValueError for unrecognized directories
            if type(e) is ValueError:
                run = None
            else:
                sys.stderr.write("Error while loading run %s\n" % run_dir)
                raise e
        return(run)

    def load_run_data(self):
        """Match up Run directories with per-experiment metadata."""
        run_dirs = [d for d in self.path_runs.glob("*") if d.is_dir()]
        runs = [self.run_setup_with_checks(run_dir) for run_dir in run_dirs]
        # Ignore unrecognized (None) entries
        runs = [run for run in runs if run]
        for run in runs:
            for al in run.alignments:
                self.match_alignment_to_projects(al)
        self.runs = runs
        return(runs)

    def watch_and_process(self, poll=5):
        """Regularly check for new data and enqueue projects for processing."""
        self._finish_up = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        while not self._finish_up:
            # TODO self.refresh()
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
            proj.status = project.ProjectData.PROCESSING
            # TODO actual processing!
            time.sleep(1)
            proj.status = project.ProjectData.COMPLETE
            self._queue.task_done()
