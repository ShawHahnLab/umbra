"""Do nothing at all.

This is a minimal working example of a task module.  See also task_fail.py."""

from umbra import task

class TaskNoop(task.Task):
    """Do nothing at all."""

    order = 0

    def run(self):
        pass
