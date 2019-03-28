"""Fail processing by raising an exception.

This is a controlled failure case for testing and troubleshooting.  See also
task_noop.py."""

from umbra.util import ProjectError
from umbra import task

class TaskFail(task.Task):
    """Fail processing by raising an exception."""

    order = 1

    def run(self):
        raise ProjectError("Failing ProjectData as requested")
