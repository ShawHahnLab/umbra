"""Copy the run directory into the processing directory."""

from distutils.dir_util import copy_tree
from umbra import task

class TaskCopy(task.Task):
    """Copy the run directory into the processing directory."""

    # pylint: disable=no-member
    order = 2

    def run(self):
        src = str(self.proj.analysis.run.path)
        dest = str(self.task_dir_parent(self.name) /
                   self.proj.analysis.run.run_id)
        copy_tree(src, dest)
