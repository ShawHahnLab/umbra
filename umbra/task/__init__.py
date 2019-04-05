"""
The core task functionality for processing.

Add-on tasks should be subclasses of the base class Task.  Tasks are
instantiated by ProjectData objects for individual datasets.
"""

# https://docs.python.org/3/reference/datamodel.html

import re
import importlib
import pkgutil
import sys
import logging
import inspect
import subprocess
import traceback
from pathlib import Path
from ..util import mkparent

# requires on PATH:
#  * cutadapt
#  * spades.py

LOGGER = logging.getLogger(__name__)

def task_classes():
    """A dict of available Task classes in this module, by name.

    This relies on the "name" attribute of each object beginning with "Task" in
    this module, and excludes the TaskParent meta-class and the Task base
    class."""
    objs = globals()
    keeps = lambda n: n.startswith("Task") and n != "Task"
    tasks = {objs[n].name: objs[n] for n in objs if keeps(n)}
    return tasks

def __load_task_classes():
    """Load classes from within this package whose names start with "Task".

    The idea here is that each task is in its own module, largely independent
    except for what the task class inherits from its parents.  After this is
    executed, task_classes() should produce a complete dictionary of available
    tasks.
    """
    package = sys.modules[__package__]
    for _, modname, _ in pkgutil.walk_packages(package.__path__,
                                               package.__name__+"."):
        mod = importlib.import_module(modname, __package__)
        for name in dir(mod):
            if name.startswith("Task"):
                LOGGER.debug("Importing into %s: %s (from %s)",
                             __package__, name, mod)
                globals()[name] = getattr(mod, name)


# pylint: disable=invalid-name,too-few-public-methods
class classproperty():
    """
    Based on:
    https://stackoverflow.com/a/5192374
    https://stackoverflow.com/a/5191224
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, owner):
        return self.func(owner)


# pylint: disable=invalid-name
class __TaskParent(type):
    """A class for Task classes.

    This lets us add some class-level automatic magic to any Task classes.
    There's nothing much to see here, you probably want Task instead.
    """

    def __lt__(cls, cls_other):
        """Define "<" operator which allows easy task sorting by order."""
        # https://stackoverflow.com/a/4010558
        return cls.order < cls_other.order

    def __gt__(cls, cls_other):
        """Define ">" operator."""
        return cls.order > cls_other.order

    def __ge__(cls, cls_other):
        """Define ">=" operator."""
        return cls.order >= cls_other.order

    def __le__(cls, cls_other):
        """Define "<=" operator."""
        return cls.order <= cls_other.order

    def __new__(cls, clsname, superclasses, attrs):
        # Define a name attribute for the class.  I'm doing it this roundabout
        # meta way so that we can easily have each Task class have a name
        # attribute (and not just each instance of the class) automatically.
        name = clsname.lower()
        name = name.replace("task", "", 1) or name
        attrs["name"] = name

        def __get_source_path(cls):
            """A reference to the Path of the module the class came from.

            For built-in tasks, relative to the package. For custom tasks,
            absolute path.
            """
            # Useful when we have a mix of built-in and custom tasks.
            # Note that I think we need to define this as a function so it
            # refers to the correct module at run-time.
            # https://stackoverflow.com/a/12154601
            parent = Path(__file__).parent
            path = Path(inspect.getfile(cls))
            try:
                path = path.relative_to(parent)
            except ValueError:
                pass
            return path

        attrs["__get_source_path"] = __get_source_path
        attrs["source_path"] = classproperty(__get_source_path)
        attrs["__lt__"] = cls.__lt__
        attrs["__gt__"] = cls.__gt__
        attrs["__le__"] = cls.__le__
        attrs["__ge__"] = cls.__ge__

        return type.__new__(cls, clsname, superclasses, attrs)


class Task(metaclass=__TaskParent):
    """Base class for a processing task.

    To create a custom proessing task, make a sub-class of this class,
    following the instructions here.  Set the docstring for the class to a
    basic description of what the task does.

    Use the helper method available via this class to access file paths and run
    information.
    """

    # Task execution order.
    # A higher number means run later than tasks with lower numbers.  This
    # default setting will run after the core processing tasks but before the
    # final package/upload/email set of tasks.
    order = 100

    # List of task names to implicitly requre.
    # Tasks in the returned list will be automatically included in the set of
    # tasks executed.  (Note that run order is determined by each task's order
    # property; there is no real resolution of dependencies in a graph-like
    # way.)
    dependencies = []

    @classproperty
    def summary(cls):
        """Brief text summary of main Task attributes."""
        fmt = "name: %s\norder: %s\ndependencies: %s\nsource_path: %s"
        info = fmt % (cls.name, cls.order, ", ".join(cls.dependencies), cls.source_path)
        return info

    @property
    def log_path(self):
        """Path to log file for this task."""
        path = (self.proj.path_proc /
                self.proj.config.get("log_path", "logs") /
                ("log_" + self.name + ".txt")).resolve()
        return path

    @property
    def nthreads(self):
        """Max number of threads to be used in processing.

        This is just an integer hint for any subprocesses started here.  The
        task itself will always run within a single thread.
        """
        return self.proj.nthreads

    def __init__(self, config, proj):
        """Task object initlization.

        When a task is created it will be given a dictionary of configuration
        information and a project data object with a bundle of run information
        and metadata.  If you override __init__ be sure to
        super().__init__(config, proj).
        """
        self.config = config
        self.proj = proj
        self.logf = None

    def __del__(self):
        if self.logf:
            self.logf.close()

    def run(self):
        """The core functionality for the task.

        Override this method in your own task.  This will be called when it is
        time to actually execute the task.

        Any exceptions raised once a task is initialized are caught during
        processing, logged, and used to mark the project as failed.  If run()
        completes without exceptions success is assumed and the next task is
        executed.  umbra.util.ProjectError can be used for recognized types of
        failure, but it's treated the same as any exception.

        The return value from here will be added to the dictionary of
        processing information for the project and serialized to YAML on disk,
        so if giving a return value be sure to use just basic types that won't
        cause trouble (no arbitrary objects).  The None object is fine.
        """
        raise NotImplementedError

    def runcmd(self, args, stdout=None, stderr=None):
        """A simple wrapper to execute a command.

        This will call the command specified by the list of arguments, with the
        standard output and standard error streams defaulting to the task's
        open log file.  Any non-zero exit code will result in a
        subprocess.CalledProcessError being raised.  See also: subprocess.run.
        """
        if not stdout:
            self.log_setup()
            stdout = self.logf
        if not stderr:
            self.log_setup()
            stderr = self.logf
        subprocess.run(args, stdout=stdout, stderr=stderr, check=True)

    @property
    def sample_paths(self):
        """Dict mapping sample names to filesystem paths."""
        return self.proj.sample_paths

    def task_path(self, readfile, taskname, subdir, suffix="", r1only=True):
        """Give readfile-related path, following the originals' name."""
        pat = "(.*_L[0-9]+_)R([12])(_001)\\.fastq\\.gz"
        if r1only:
            name = re.sub(pat, "\\1R\\3" + suffix, readfile.name)
        else:
            name = re.sub(pat, "\\1R\\2\\3" + suffix, readfile.name)
        fastq_out = self._task_dir_parent(taskname) / subdir / name
        mkparent(fastq_out)
        return fastq_out

    def _task_dir_parent(self, taskname):
        """Give processing parent path for a given task.

        This will take into account configuration settings for the project and
        the specific task, if present.  If all defaults are set this will just
        be the processing directory.
        """
        path_implicit = "."
        if self._task_is_implicit(taskname):
            path_implicit = self.proj.config.get("implicit_tasks_path", ".")
        path = (self.proj.path_proc / path_implicit).resolve()
        return path

    def _task_is_implicit(self, taskname):
        """True if a task was implicitly included, False otherwise.

        If tasks are in the list given for a project, are in the
        always_explicit_tasks list, or are in the task_null list, they are
        considered explicitly-requested.  Other cases (e.g. dependencies of
        explicit tasks or the default tasks) are considered implicit.  This can
        affect the output file paths for a given task.
        """
        explicit_tasks = (
            self.proj.experiment_info["tasks"] +
            self.proj.config.get("always_explicit_tasks", []) +
            self.proj.config.get("task_null", []))
        return taskname not in explicit_tasks

    def log_setup(self):
        """Open the log file for writing."""
        mkparent(self.log_path)
        try:
            if not self.logf.writable():
                self.logf = open(self.log_path, "w")
        except AttributeError:
            self.logf = open(self.log_path, "w")

    def runwrapper(self):
        """Setup/cleanup around each Task's run method.

        Don't worry about this one, just override run().
        """
        self.log_setup()
        try:
            return self.run()
        except Exception as exception:
            msg = traceback.format_exc()
            self.logf.write(msg + "\n")
            raise exception

__load_task_classes()
