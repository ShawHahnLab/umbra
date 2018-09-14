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
            for row in experiment_info:
                name = row["Project"]
                if not name in projects.keys():
                    al_idx = str(alignment.index)
                    run_id = alignment.run.run_id
                    proj_file = slugify(name) + ".yml"
                    fp = Path(dp_align) / run_id / al_idx / proj_file
                    projects[name] = ProjectData(name, fp, alignment, exp_path)
                projects[name]._add_exp_row(row) 
        return(projects)

    def __init__(self, name, path, alignment, exp_path):
        self.name = name
        self.alignment = alignment
        self.metadata = {"status": ProjectData.NONE}
        self.path = path
        #self.sample_paths = None
        exp_info = {
                "sample_names": [],
                "tasks": [],
                "contacts": dict()
                }
        self.metadata["alignment_info"] = {}
        self.metadata["experiment_info"] = exp_info
        self.metadata["run_info"] = {}
        self.metadata["sample_paths"] = {}
        self.metadata["tasks_pending"] = []
        self.metadata["tasks_completed"] = []
        self.metadata["current_task"] = ""
        if self.alignment:
            self.metadata["alignment_info"]["path"] = str(self.alignment.path)
            self.metadata["experiment_info"]["path"] = exp_path
            self.metadata["experiment_info"]["name"] = self.alignment.experiment
        if self.alignment.run:
            self.metadata["run_info"]["path"] = str(self.alignment.run.path)
        self.load_metadata()

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
        return(self.metadata["tasks_pending"])

    def process(self):
        """Run all tasks.
        
        This function will block until processing is complete."""
        # TODO see illumina's python task library on github maybe?
        self.status = ProjectData.PROCESSING
        # first match up tasks with the ordered list and include any
        # dependencies.
        tasks = self.experiment_info["tasks"]
        tasks = self.normalize_tasks(tasks)
        self.metadata["tasks_pending"] = tasks
        self.save_metadata()
        while self.tasks_pending:
            self._process_task()
        self.status = ProjectData.COMPLETE

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

    def _process_task(self):
        """Process the next pending task."""
        # TODO: pop task off of tasks_pending, marking it as current_task, and
        # then put it on tasks_completed.  Make sure to save (make this
        # transparent) at each step.
        self.metadata["tasks_completed"] = self.metadata["tasks_pending"]
        self.metadata["tasks_pending"] = []
        self.metadata["current_task"] = ""

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

    def _add_exp_row(self, row):
        """Add data from given experiment data entry.
        
        Assumes the data is relevant for this project."""
        exp_info = self.metadata["experiment_info"]
        sample_name = row["Sample_Name"].strip()
        if not sample_name in exp_info["sample_names"]:
            exp_info["sample_names"].append(sample_name)
        exp_info["contacts"].update(row["Contacts"])
        for task in row["Tasks"]:
            if not task in exp_info["tasks"]:
                exp_info["tasks"].append(task)
