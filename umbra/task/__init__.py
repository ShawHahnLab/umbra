"""
The core task functionality for processing.

Add-on tasks should be subclasses of the base class Task.  Tasks are
instantiated by ProjectData objects for individual datasets.
"""

import re
import importlib
import pkgutil
import sys
import logging
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


# pylint: disable=invalid-name
class __TaskParent(type):
    """A class for Task classes.

    This lets us add some class-level automatic magic to any Task classes.
    There's nothing much to see here, you probably want Task instead.
    """

    def __new__(cls, clsname, superclasses, attrs):
        # Define a name attribute for the class.  I'm doing it this roundabout
        # meta way so that we can easily have each Task class have a name
        # attribute (and not just each instance of the class) automatically.
        attrs["name"] = clsname
        name = clsname.replace("Task", "", 1).lower()
        if name != "":
            attrs["name"] = name
        # Store a reference to the original module the class came from.  Useful
        # when we have a mix of built-in and custom tasks.
        # TODO hmm, doesn't work quite right yet.  Everything shows up as here.
        # Maybe because of my namespace-mangling magic?  Or because the package
        # path really is always the same?
        #attrs["task_module"] = Path(package.__path__[0])

        def task_module():
            package = sys.modules[__package__]
            print(dir(package))
            print(__file__)
            print(__name__)
            print(__path__)
            print('')

        attrs["task_module"] = task_module

        return type.__new__(cls, clsname, superclasses, attrs)


class Task(metaclass=__TaskParent):
    """Base class for a processing task.

    To create a custom proessing task, make a sub-class of this class,
    following the instructions here.  Set the docstring for the class to a
    basic description of what the task does.

    Use the helper method available via this class to access file paths and run
    information.
    """

    @property
    def name(self):
        """Name of this task, based on its class."""
        return self.__class__.name

    def __lt__(self, other):
        """Define "<" operator which allows easy task sorting by order."""
        # https://stackoverflow.com/a/4010558
        return self.order < other.order

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

    @property
    def log_path(self):
        """Path to log file for this task."""
        path = (self.proj.path_proc /
                self.proj.config.get("log_path", "logs") /
                ("log_" + self.name + ".txt")).resolve()
        mkparent(path)
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
        and metadata.  If you override __init__ be sure to handle this part or
        super().__init__(config, proj).
        """
        self.config = config
        self.proj = proj

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
        if taskname not in self.proj.experiment_info["tasks"]:
            path_implicit = self.proj.config.get("implicit_tasks_path", ".")
        path = (self.proj.path_proc / path_implicit).resolve()
        return path

__load_task_classes()
