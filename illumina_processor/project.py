from .util import *
from . import experiment
import yaml
import os

class ProjectData:
    """The subset of files for a Run and Alignment specific to one project.
    
    This references the data files within a specific run relevant to a single
    project, tracks the associated additional metadata provided via the
    "experiment" identified in the sample sheet, and handles post-processing.

    The same project may span many separate runs, but a ProjectData object
    refers only to a specific portion of a single Run.
    """
    # TODO pass in experiment metadata for relevant samples
    # track run/alignment_num/project_name at this point.


    # ProjectData processing status enumerated type.
    NONE          = "none"
    PROCESSING    = "processing"
    PACKAGE_READY = "package-ready"
    COMPLETE      = "complete"
    STATUS = [NONE, PROCESSING, PACKAGE_READY, COMPLETE]

    # Recognized tasks:
    TASK_NOOP     = "noop"     # do nothing
    TASK_PASS     = "pass"     # copy raw files
    TASK_TRIM     = "trim"     # trip adapters
    TASK_ASSEMBLE = "assemble" # contig assembly
    TASK_PACKAGE  = "package"  # package in zip file
    TASK_UPLOAD   = "upload"   # upload to Box

    # Adding an explicit order and dependencies.
    TASKS = [
            TASK_NOOP,
            TASK_PASS,
            TASK_TRIM,
            TASK_ASSEMBLE,
            TASK_PACKAGE,
            TASK_UPLOAD]

    TASK_DEPS = {
            TASK_UPLOAD: [TASK_PACKAGE],
            TASK_ASSEMBLE: [TASK_TRIM],
            }

    # We'll always include these tasks:
    TASK_DEFAULTS = [
            TASK_UPLOAD
            ]

    def from_alignment(alignment, path_exp, dp_align):
        """Make dict of ProjectData objects from alignment/experiment table."""
        # Row by row, build up a dict for each unique project.  Even though
        # we're reading it in as a spreadsheet we'll treat most of this as
        # an unordered sets for each project.

        exp_path = path_exp / alignment.experiment / "metadata.csv"
        projects = {}
        try:
            # Load the spreadsheet of per-sample project information
            experiment_info = experiment.load_metadata(exp_path)
        except FileNotFoundError:
            pass
        else:
            names = [row["Project"] for row in experiment_info]
            run_id = alignment.run.run_id
            al_idx = str(alignment.index)
            for name in names:
                proj_file = slugify(name) + ".yml"
                fp = Path(dp_align) / run_id / al_idx / proj_file
                projects[name] = ProjectData(name, fp, alignment,
                        experiment_info,
                        exp_path)
        return(projects)

    def __init__(self, name, path, alignment, exp_info_full, exp_path=None):

        self.name = name
        self.alignment = alignment
        self.path = path
        self.metadata = {"status": ProjectData.NONE}
        # TODO tidy up these keys and protect behind getters/setters with
        # automatic file updating.
        self.metadata["alignment_info"] = {}
        self.metadata["experiment_info"] = self._setup_exp_info(exp_info_full)
        self.metadata["experiment_info"]["path"] = str(exp_path or "")
        self.metadata["run_info"] = {}
        self.metadata["sample_paths"] = {}
        self.metadata["task_status"] = self._setup_task_status()
        if self.alignment:
            self.metadata["alignment_info"]["path"] = str(self.alignment.path)
            self.metadata["experiment_info"]["name"] = self.alignment.experiment
        if self.alignment.run:
            self.metadata["run_info"]["path"] = str(self.alignment.run.path)
        self.load_metadata()
        self.save_metadata()

    def _setup_exp_info(self, exp_info_full):
        exp_info = {
                "sample_names": [],
                "tasks": [],
                "contacts": dict()
                }
        for row in exp_info_full:
            if row["Project"] is self.name:
                sample_name = row["Sample_Name"].strip()
                if not sample_name in exp_info["sample_names"]:
                    exp_info["sample_names"].append(sample_name)
                exp_info["contacts"].update(row["Contacts"])
                for task in row["Tasks"]:
                    if not task in exp_info["tasks"]:
                        exp_info["tasks"].append(task)
        return(exp_info)

    def _setup_task_status(self):
        tasks = self.experiment_info["tasks"]
        tasks = self.normalize_tasks(tasks)
        task_status = {}
        task_status["pending"] = tasks
        task_status["completed"] = []
        task_status["current"] = ""
        return(task_status)

    @property
    def status(self):
        return(self.metadata["status"])

    @status.setter
    def status(self, value):
        if not value in ProjectData.STATUS:
            raise ValueError
        self.metadata["status"] = value
        self.save_metadata()

    @property
    def experiment_info(self):
        return(self.metadata["experiment_info"])

    @property
    def tasks_pending(self):
        return(self.metadata["task_status"]["pending"])

    def normalize_tasks(self, tasks):
        """Add default and dependency tasks and order as needed."""
        # Add in any defaults.  If nothing is given, always include PASS.
        if not tasks:
            tasks = [ProjectData.TASK_PASS]
        tasks += ProjectData.TASK_DEFAULTS
        # Add in dependencies for any that exist.
        deps = [ProjectData.TASK_DEPS.get(t, []) for t in tasks]
        tasks = sum(deps, tasks)
        # Keep unique only.
        tasks = list(set(tasks))
        # order and verify.  We'll get a ValueError from index() if any of
        # these are not found.
        indexes = [ProjectData.TASKS.index(t) for t in tasks]
        tasks = [t for _,t in sorted(zip(indexes, tasks))]
        return(tasks)

    def process(self):
        """Run all tasks.
        
        This function will block until processing is complete."""
        self.status = ProjectData.PROCESSING
        task = self._task_next()
        while task: 
            self.process_task(task)
            task = self._task_next()
        self.status = ProjectData.COMPLETE

    def process_task(self, task):
        """Process the next pending task."""
        sys.stderr.write(task + "\n")

    def _task_next(self):
        """Mark the next task as current and update the task status."""
        ts = self.metadata["task_status"]
        if ts["current"]:
            # TODO don't do this; move current to completed carefully, in
            # process_task.
            ts["completed"] = ts["current"]
            ts["current"] = ts["pending"].pop(0)
        self.save_metadata()
        return(ts["current"])

    def load_metadata(self, fp=None):
        if fp:
            self.path = fp
        try:
            with open(self.path) as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            data = None
        else:
            self.metadata.update(data)
        return(data)

    def save_metadata(self):
        """Update project metadata on disk."""
        # We need to be careful because other threads may be working in the
        # exact same directory right now.  pathlib didn't handle this correctly
        # until recently:
        # https://bugs.python.org/issue29694
        # I'll stick with the os module for now, since it seems to handle it
        # just fine.
        dp = Path(self.path.parent)
        os.makedirs(dp, exist_ok=True)
        with open(self.path, "w") as f:
            f.write(yaml.dump(self.metadata))

    @property
    def sample_paths(self):
        paths = self.metadata["sample_paths"]
        if not paths: 
            return({})
        paths2 = {}
        for k in paths:
            paths2[k] = [Path(p) for p in paths[k]]
        return(paths2)

    def set_sample_paths(self, sample_paths):
        if sample_paths:
            self.metadata["sample_paths"] = {}
            for sample_name in self.metadata["experiment_info"]["sample_names"]:
                paths = [str(p) for p in sample_paths[sample_name]]
                self.metadata["sample_paths"][sample_name] = paths
        else:
            self.metadata["sample_paths"] = None
