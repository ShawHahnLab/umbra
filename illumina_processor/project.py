from .util import *
from . import experiment
import yaml
import traceback
from zipfile import ZipFile
import subprocess

class ProjectError(Exception):
    """Any sort of project-related exception."""
    pass

class ProjectData:
    """The data for a Run and Alignment specific to one project.
    
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
    FAILED        = "failed"
    STATUS = [NONE, PROCESSING, PACKAGE_READY, COMPLETE, FAILED]

    # Recognized tasks:
    TASK_NOOP     = "noop"     # do nothing
    TASK_COPY     = "copy"     # copy raw files
    TASK_TRIM     = "trim"     # trip adapters
    TASK_MERGE    = "merge"    # interleave trimmed R1/R2 reads
    TASK_ASSEMBLE = "assemble" # contig assembly
    TASK_PACKAGE  = "package"  # package in zip file
    TASK_UPLOAD   = "upload"   # upload to Box

    # Adding an explicit order and dependencies.
    TASKS = [
            TASK_NOOP,
            TASK_COPY,
            TASK_TRIM,
            TASK_MERGE,
            TASK_ASSEMBLE,
            TASK_PACKAGE,
            TASK_UPLOAD
            ]

    TASK_DEPS = {
            TASK_UPLOAD: [TASK_PACKAGE],
            TASK_MERGE: [TASK_TRIM],
            TASK_ASSEMBLE: [TASK_MERGE],
            }

    # We'll always include these tasks:
    TASK_DEFAULTS = [
            TASK_PACKAGE,
            TASK_UPLOAD
            ]

    # If nothing else is given, this will be added:
    TASK_NULL = [
            TASK_COPY
            ]

    def from_alignment(alignment, path_exp, dp_align, dp_proc, dp_pack):
        """Make dict of ProjectData objects from alignment/experiment table."""
        # Row by row, build up a dict for each unique project.  Even though
        # we're reading it in as a spreadsheet we'll treat most of this as
        # an unordered sets for each project.

        exp_path = path_exp / alignment.experiment / "metadata.csv"
        projects = set()
        try:
            # Load the spreadsheet of per-sample project information
            experiment_info = experiment.load_metadata(exp_path)
        except FileNotFoundError:
            pass
        else:
            sample_paths = None
            try:
                sample_paths = alignment.sample_paths()
            except FileNotFoundError as e:
                msg = "\nFASTQ file not found:\n"
                msg += "Run:       %s\n" % alignment.run.path
                msg += "Alignment: %s\n" % alignment.path
                msg += "File:      %s\n" % e.filename
                warnings.warn(msg)
            # Set of unique names in the experiment spreadsheet
            names = {row["Project"] for row in experiment_info}
            run_id = alignment.run.run_id
            al_idx = str(alignment.index)
            for name in names:
                proj_file = slugify(name) + ".yml"
                fp = Path(dp_align) / run_id / al_idx / proj_file
                proj = ProjectData(name, fp,
                        dp_proc,
                        dp_pack,
                        alignment,
                        experiment_info,
                        exp_path)
                proj.sample_paths = sample_paths
                projects.add(proj)
        return(projects)

    def __init__(self, name, path, dp_proc, dp_pack, alignment, exp_info_full, exp_path=None):

        self.name = name
        self.alignment = alignment
        self.path = path # YAML metadata path
        self.metadata = {"status": ProjectData.NONE}
        self.readonly = self.path.exists()
        self.load_metadata()
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

        self.path_proc = Path(dp_proc) / self.work_dir
        self.path_pack = Path(dp_pack) / (self.work_dir + ".zip")
        if not self.readonly:
            self.save_metadata()

    @property
    def status(self):
        return(self.metadata["status"])

    @status.setter
    def status(self, value):
        if not value in ProjectData.STATUS:
            raise ValueError
        self.metadata["status"] = value
        if not self.readonly:
            self.save_metadata()

    @property
    def experiment_info(self):
        return(self.metadata["experiment_info"])

    @property
    def work_dir(self):
        """Short name for working directory, without path"""
        txt_date = datestamp(self.alignment.completion_time)
        txt_proj = slugify(self.name)
        # first names of contacts given
        who = self.experiment_info["contacts"]
        who = [txt.split(" ")[0] for txt in who.keys()]
        who = "-".join(who)
        txt_name = slugify(who)
        dirname = "%s-%s-%s" % (txt_date, txt_proj, txt_name)
        return(dirname)

    @property
    def sample_paths(self):
        paths = self.metadata["sample_paths"]
        if not paths: 
            return({})
        paths2 = {}
        for k in paths:
            paths2[k] = [Path(p) for p in paths[k]]
        return(paths2)

    @sample_paths.setter
    def sample_paths(self, sample_paths):
        if sample_paths:
            self.metadata["sample_paths"] = {}
            for sample_name in self.metadata["experiment_info"]["sample_names"]:
                paths = [str(p) for p in sample_paths[sample_name]]
                self.metadata["sample_paths"][sample_name] = paths
        else:
            self.metadata["sample_paths"] = None

    @property
    def tasks_pending(self):
        return(self.metadata["task_status"]["pending"])

    @property
    def tasks_completed(self):
        return(self.metadata["task_status"]["completed"])

    @property
    def task_current(self):
        return(self.metadata["task_status"]["current"])

    def deps_completed(self, task):
        """ Are all dependencies of a given task already completed?"""
        deps = ProjectData.TASK_DEPS.get(task, [])
        remaining = [d for d in deps if d not in self.tasks_completed]
        return(not remaining)

    def process(self):
        """Run all tasks.
        
        This function will block until processing is complete.  Calling process
        if readyonly=True raises ProjectError."""
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
        self.status = ProjectData.PROCESSING
        ts = self.metadata["task_status"]
        while self.tasks_pending: 
            if self.task_current:
                raise ProjectError("a task is already running")
            ts["current"] = ts["pending"].pop(0)
            if not self.deps_completed(ts["current"]):
                raise ProjectError("not all dependencies for task completed")
            self.save_metadata()
            try:
                self._run_task(ts["current"])
            except Exception as e:
                self.metadata["failure_exception"] = traceback.format_exc()
                self.status = ProjectData.FAILED
                raise(e)
            ts["completed"].append(ts["current"])
            ts["current"] = ""
            self.save_metadata()
        self.status = ProjectData.COMPLETE

    def load_metadata(self):
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
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
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

    # Task-related methods

    def copy_run(self):
        """Copy the run directory into the processing directory."""
        src = str(self.alignment.run.path)
        dest = str(self.path_proc / self.alignment.run.run_id)
        copy_tree(src, dest)

    def _trimmed_path(self, path_fastq):
        name = re.sub("\\.fastq\\.gz$", "", path_fastq.name)
        fastq_out = self.path_proc / "trimmed" / (name + ".trimmed.fastq")
        mkparent(fastq_out)
        return(fastq_out)

    def _log_path(self, task):
        path = self.path_proc / "logs" / ("log_" + str(task) + ".txt")
        mkparent(path)
        return(path)

    def trim(self):
        # For each sample, separately process the one or more associated files.
        # TODO open trim log
        with open(self._log_path("trim"), "w") as f:
            for samp in self.sample_paths.keys():
                paths = self.sample_paths[samp]
                if len(paths) > 2:
                    raise ProjectError("trimming can't handle >2 files per sample")
                for i in range(len(paths)):
                    adapter = illumina.adapters["Nextera"][i]
                    fastq_in = str(paths[i])
                    fastq_out = self._trimmed_path(paths[i])
                    args = ["cutadapt", "-a", adapter, "-o", fastq_out, fastq_in]
                    # Call cutadapt with each file.  If the exit status is
                    # nonzero or if the expected output file is missing, raise
                    # an exception.
                    subprocess.run(args, stdout=f, stderr=f, check=True)
                    if not Path(fastq_out).exists():
                        msg = "missing output file %s" % fastq_out
                        raise ProjectError(msg)

    def assemble(self):
        raise NotImplementedError("assemble")

    def zip(self):
        """Create zipfile of processing directory and metadata."""
        with ZipFile(self.path_pack, "x") as z:
            # Archive everything in the processing directory
            for root, dirs, files in os.walk(self.path_proc):
                for fn in files:
                    # Archive the file but trim the name so it's relative to
                    # the processing directory.
                    filename = os.path.join(root, fn)
                    arcname = Path(filename).relative_to(self.path_proc.parent)
                    z.write(filename, arcname)
            # Also add in a copy of the metadata YAML file as it currently
            # stands.
            filename = self.path
            arcname = Path(self.path_proc.name) / ("." + str(filename.name))
            z.write(filename, arcname)

    # Implementation Details

    def _setup_exp_info(self, exp_info_full):
        exp_info = {
                "sample_names": [],
                "tasks": [],
                "contacts": dict()
                }
        for row in exp_info_full:
            if row["Project"] == self.name:
                sample_name = row["Sample_Name"].strip()
                if not sample_name in exp_info["sample_names"]:
                    exp_info["sample_names"].append(sample_name)
                exp_info["contacts"].update(row["Contacts"])
                for task in row["Tasks"]:
                    if not task in exp_info["tasks"]:
                        exp_info["tasks"].append(task)
        return(exp_info)

    def _setup_task_status(self):
        tasks = self.experiment_info["tasks"][:]
        tasks = self._normalize_tasks(tasks)
        task_status = {}
        task_status["pending"] = tasks
        task_status["completed"] = []
        task_status["current"] = ""
        return(task_status)

    def _normalize_tasks(self, tasks):
        """Add default and dependency tasks and order as needed."""
        # Add in any defaults.  If nothing is given, always include the
        # default.
        if not tasks:
            tasks = ProjectData.TASK_NULL[:]
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

    def _run_task(self, task):
        """Process the next pending task."""
        
        # No-op: do nothing!
        if task == ProjectData.TASK_NOOP:
            pass

        # Copy run directory to within processing directory
        elif task == ProjectData.TASK_COPY:
            self.copy_run()

        # Run cutadapt to trim the Illumina adapters
        elif task == ProjectData.TASK_TRIM:
            self.trim()

        # Interleave the read pairs
        elif task == ProjectData.TASK_MERGE:
            self.merge()

        # Run SPAdes to assemble contigs from the interleaved files
        elif task == ProjectData.TASK_ASSEMBLE:
            self.assemble()

        # Zip up all files in the processing directory
        elif task == ProjectData.TASK_PACKAGE:
            self.zip()

        # Upload the zip archive to Box
        elif task == ProjectData.TASK_UPLOAD:
            pass
            #raise NotImplementedError(task)

        # This should never happen (so it probably will).
        else:
            raise ProjectError("task \"%s\" not recognized" % task)
