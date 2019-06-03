"""
Handle processing of select samples from a single run.

This is the glue that connects raw sequence data and additional metadata
supplied separately, and executes any processing tasks defined.  Objects of
class ProjectData are created primarily via ProjectData.from_alignment in
IlluminaProcessor, creating an arbitrary number of ProjectData instances for a
given run as defined by the supplied metadata.csv.

A single custom exception, ProjectError, is used for any "expected" problems
that may arise (e.g., attempting to process a readonly project, existing files
in the processing output directory, unexpected input data formats, etc).
"""

import traceback
import logging
import sys
import warnings
import copy
from pathlib import Path
import yaml
from . import CONFIG
from . import config
from . import experiment
from .util import ProjectError, mkparent, slugify, datestamp, yaml_load
from . import task

LOGGER = logging.getLogger(__name__)

class ProjectData:
    """The data for a Run and Alignment specific to one project.

    This references the data files within a specific run relevant to a single
    project, tracks the associated additional metadata provided via the
    "experiment" identified in the sample sheet, and handles post-processing.

    The same project may span many separate runs, but a ProjectData object
    refers only to a specific portion of a single Run.
    """

    # ProjectData processing status enumerated type.
    NONE = "none"
    PROCESSING = "processing"
    PACKAGE_READY = "package-ready"
    COMPLETE = "complete"
    FAILED = "failed"
    STATUS = [NONE, PROCESSING, PACKAGE_READY, COMPLETE, FAILED]

    @staticmethod
    def from_alignment(alignment, path_exp, dp_align, dp_proc, dp_pack,
                       uploader, mailer, nthreads=1, readonly=False,
                       conf=None):
        """Make set of ProjectData objects from alignment/experiment table.

        This is called from IlluminaProcessor and so is protected from a set of
        "expected" errors and just logs them appropriately.  Exceptions outside
        of the expected type will still propogate so the processor will halt
        and catch fire, though."""

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
            except FileNotFoundError as exception:
                # In this case there's a mismatch in the data files expected
                # from the Alignment directory's metadata and the data files
                # actually on disk.
                msg = "\nFASTQ file not found:\n"
                msg += "Run:       %s\n" % alignment.run.path
                msg += "Alignment: %s\n" % alignment.path
                msg += "File:      %s\n" % exception.filename
                warnings.warn(msg)
            # Set of unique names in the experiment spreadsheet
            names = {row["Project"] for row in experiment_info}
            run_id = alignment.run.run_id
            al_idx = str(alignment.index)
            for name in names:
                proj_file = slugify(name) + ".yml"
                fpath = Path(dp_align) / run_id / al_idx / proj_file
                proj = ProjectData(
                    name=name,
                    path=fpath,
                    dp_proc=dp_proc,
                    dp_pack=dp_pack,
                    alignment=alignment,
                    exp_info_full=experiment_info,
                    uploader=uploader,
                    mailer=mailer,
                    exp_path=exp_path,
                    nthreads=nthreads,
                    readonly=readonly,
                    conf=conf)
                try:
                    proj.sample_paths = sample_paths
                except ProjectError:
                    # If something went wrong at this point, complain, but
                    # still add the (failed) project to the set, since it did
                    # initialize already.
                    proj.fail()
                    msg = "ProjectData setup failed for %s (%s)"
                    msg = msg % (proj.work_dir, alignment.run.run_id)
                    logging.getLogger(__name__).error(msg)
                projects.add(proj)
        return projects

    def __init__(
            self, name, path, dp_proc, dp_pack, alignment, exp_info_full,
            uploader, mailer, exp_path=None, nthreads=1, readonly=False,
            conf=None):

        self.name = name
        self.alignment = alignment
        self.exp_path = exp_path # Orig experiment spreadsheet (maybe >1 proj)
        self.path = path # YAML metadata path
        self.nthreads = nthreads # max threads to give tasks
        self.uploader = uploader # callback to upload zip file
        self.mailer = mailer # callback to send email
        # general configuration including per-task options
        self.conf = copy.deepcopy(CONFIG["task_options"])
        config.update_tree(self.conf, conf or {})
        self._metadata = {"status": ProjectData.NONE}
        self.readonly = self.path.exists() or readonly
        self.load_metadata()
        self._metadata["alignment_info"] = {}
        self._metadata["experiment_info"] = self._setup_exp_info(exp_info_full)
        self._metadata["experiment_info"]["path"] = str(exp_path or "")
        self._metadata["run_info"] = {}
        self._metadata["sample_paths"] = {}
        self.tasks = self._setup_tasks()
        self._metadata["task_status"] = self._setup_task_status()
        self._metadata["task_output"] = {}
        if self.alignment:
            self._metadata["alignment_info"]["path"] = str(self.alignment.path)
            self._metadata["experiment_info"]["name"] = self.alignment.experiment
        if self.alignment.run:
            self._metadata["run_info"]["path"] = str(self.alignment.run.path)
        self._metadata["work_dir"] = self._init_work_dir_name()

        self.path_proc = Path(dp_proc) / self.work_dir
        self.path_pack = Path(dp_pack) / (self.work_dir + ".zip")
        if not self.readonly:
            if self.path_proc.exists() and self.path_proc.glob("*"):
                LOGGER.warning(
                    "Processing directory exists and is not empty: %s",
                    str(self.path_proc))
                LOGGER.warning(
                    "Marking project readonly: %s", self.work_dir)
                self.readonly = True
            else:
                self.save_metadata()
        LOGGER.info("ProjectData initialized: %s", self.work_dir)

    def _init_work_dir_name(self):
        txt_date = datestamp(self.alignment.run.rta_complete["Date"])
        txt_proj = slugify(self.name)
        # first names of contacts given
        who = self.experiment_info["contacts"]
        who = [txt.split(" ")[0] for txt in who.keys()]
        who = "-".join(who)
        txt_name = slugify(who)
        fields = [txt_date, txt_proj, txt_name]
        fields = [f for f in fields if f]
        dirname = "-".join(fields)
        if not dirname:
            raise ProjectError("empty work_dir")
        return dirname

    @property
    def status(self):
        """Get processing status with shorthand method."""
        return self._metadata["status"]

    @status.setter
    def status(self, value):
        """Set status to an allowed value and update metadata on disk."""
        if not value in ProjectData.STATUS:
            raise ValueError
        self._metadata["status"] = value
        if not self.readonly:
            self.save_metadata()

    @property
    def experiment_info(self):
        """Dict of experiment metdata specific to this project."""
        return self._metadata["experiment_info"]

    @property
    def task_output(self):
        """Dict of task output data."""
        return self._metadata["task_output"]

    @property
    def work_dir(self):
        """Short name for working directory, without path."""
        return self._metadata["work_dir"]

    @property
    def contacts(self):
        """Dictionary of user contact info from experiment metadata."""
        return self._metadata["experiment_info"]["contacts"].copy()

    @property
    def sample_paths(self):
        """Dict mapping sample names to filesystem paths."""
        paths = self._metadata["sample_paths"]
        if not paths:
            return {}
        paths2 = {}
        for k in paths:
            paths2[k] = [Path(p) for p in paths[k]]
        return paths2

    @sample_paths.setter
    def sample_paths(self, sample_paths):
        # Here we expect that each sample name listed in the metadata
        # spreadsheet will also be found in the given sample_paths from
        # the run data.  But what if not?
        # If just some, report a warning.  Maybe a typo or maybe one
        # spreadsheet is being used across multiple runs.  If all are
        # missing, throw exception.
        # Keep paths for the sample names present in both the experiment
        # metadata spreadsheet and the given sample paths by sample name.
        # Exclude sample names we didn't find in the given sample paths.
        # (Also implicitly excludes unrelated sample paths that may be for
        # other projects.)
        from_exp = set(self.experiment_info["sample_names"])
        from_given = set(sample_paths.keys())
        keepers = from_exp & from_given
        excluded = from_exp - keepers
        msg = "Samples from experiment/run/both: %d/%d/%d"
        msg = msg % (len(from_exp), len(from_given), len(keepers))
        try:
            if excluded and keepers:
                # some excluded, some kept
                msg += " (some not matched in run)"
                lvl = logging.WARNING
            elif excluded:
                # all excluded, none kept
                msg += " (none matched in run)"
                lvl = logging.ERROR
                err = "No matching samples between experiment and run"
                raise ProjectError(err)
            elif keepers:
                # OK! all kept.
                lvl = logging.DEBUG
            else:
                # huh, nothing given here?  warning?
                msg += " (none given in run)"
                lvl = logging.WARNING
        finally:
            # In any case, log the sample numbers at the appropriate level.  An
            # exception will continue from here, if present.
            LOGGER.log(lvl, msg)
        self._metadata["sample_paths"] = {}
        for sample_name in keepers:
            paths = [str(p) for p in sample_paths[sample_name]]
            self._metadata["sample_paths"][sample_name] = paths

    @property
    def tasks_pending(self):
        """List of task names not yet completed."""
        return self._metadata["task_status"]["pending"]

    @property
    def tasks_completed(self):
        """List of task names completed."""
        return self._metadata["task_status"]["completed"]

    @property
    def task_current(self):
        """Name of task currently running."""
        return self._metadata["task_status"]["current"]

    def deps_completed(self, taskname):
        """ Are all dependencies of a given task already completed?"""
        deps = self.__deps_for(taskname)
        remaining = [d for d in deps if d not in self.tasks_completed]
        return not remaining

    def process(self):
        """Run all tasks.

        This function will block until processing is complete.  Calling process
        if readyonly=True or status != NONE raises ProjectError."""
        LOGGER.info("ProjectData processing: %s", self.work_dir)
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
        elif self.status != ProjectData.NONE:
            msg = "ProjectData status already defined as \"%s\"" % self.status
            raise ProjectError(msg)
        self.status = ProjectData.PROCESSING
        try:
            self.path_proc.mkdir(parents=True, exist_ok=True)
            tstat = self._metadata["task_status"]
            while self.tasks_pending:
                if self.task_current:
                    raise ProjectError("a task is already running")
                tstat["current"] = tstat["pending"].pop(0)
                if not self.deps_completed(tstat["current"]):
                    raise ProjectError("not all dependencies for task completed")
                self.save_metadata()
                self._run_task(tstat["current"])
                tstat["completed"].append(tstat["current"])
                tstat["current"] = ""
                self.save_metadata()
        except Exception as exception:
            self.fail()
            raise exception
        self.status = ProjectData.COMPLETE

    def fail(self):
        """Mark processing status as failed, and note exception, if any."""
        msg = ""
        if any(sys.exc_info()):
            msg = traceback.format_exc()
        self._metadata["failure_exception"] = msg
        self.status = ProjectData.FAILED

    def load_metadata(self):
        """Load metadata YAML file from disk."""
        try:
            data = yaml_load(self.path)
        except FileNotFoundError:
            data = None
        else:
            self._metadata.update(data)
        return data

    def save_metadata(self):
        """Update project metadata on disk."""
        if self.readonly:
            raise ProjectError("ProjectData is read-only")
        mkparent(self.path)
        with open(self.path, "w") as fout:
            fout.write(yaml.dump(self._metadata))

    ###### Implementation Details

    def _setup_exp_info(self, exp_info_full):
        # Row by row, build up a dict for this project.  Even though we're
        # reading the experiment info as a spreadsheet we'll treat most of this
        # as though it's unordered sets for each project.  (Not actually using
        # the set object as that gave me trouble with the YAML, but plain lists
        # do fine.)
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
                for taskname in row["Tasks"]:
                    if not taskname in exp_info["tasks"]:
                        exp_info["tasks"].append(taskname)
        return exp_info

    def _setup_tasks(self):
        """Create the list of Task objects.

        This will take into account any defaults defined and dependencies of
        each task."""
        # dictionary of name/class pairs
        tasks_all = task.task_classes()
        # explicitly-requested tasks
        tasknames = self.experiment_info["tasks"][:]
        # handle no-task case and add any defaults
        if not tasknames:
            tasknames = self.conf["task_null"][:]
        tasknames += self.conf["task_defaults"]
        # Add in dependencies for any that exist.
        deps = set()
        for taskname in tasknames:
            deps.update(self.__deps_for(taskname))
        tasknames += deps
        # Keep unique only.
        tasknames = list(set(tasknames))
        # Instantiate each one by name.  Each object gets a dedicated config
        # dictionary and a reference to this ProjectData object.
        tasks = []
        for taskname in tasknames:
            cls = tasks_all[taskname]
            task_config = self.conf["tasks"].get(taskname, {})
            obj = cls(task_config, self)
            tasks.append(obj)
        # Sort by the order attribute of each task.
        tasks = sorted(tasks)
        return tasks

    def __deps_for(self, taskname, total=None):
        """Get all dependent tasks' names (including indirect) for one task."""
        # Set of all dependencies seen so far.
        tasks_all = task.task_classes()
        if not total:
            total = set()
        for dep in tasks_all[taskname].dependencies:
            if dep not in total:
                total.update(self.__deps_for(dep, total))
            total.add(dep)
        return total

    def _setup_task_status(self):
        task_status = {}
        task_status["pending"] = [task.name for task in self.tasks]
        task_status["completed"] = []
        task_status["current"] = ""
        return task_status

    def _run_task(self, taskname):
        """Process the next pending task."""

        msg = "ProjectData processing: %s, task: %s" % (self.work_dir, taskname)
        LOGGER.debug(msg)
        # match name to object
        taskobj = next(task for task in self.tasks if task.name == taskname)
        if not taskobj:
            # This should never happen (so it probably will).
            raise ProjectError("task \"%s\" not recognized" % taskname)
        self._metadata["task_output"][taskname] = taskobj.runwrapper() or {}
