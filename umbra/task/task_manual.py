"""Wait for a "Manual" subdir to appear in the processing directory."""

import time
from umbra import task
from umbra.util import ProjectError

class TaskManual(task.Task):
    """Wait for a "Manual" subdir to appear in the processing directory.

    The run method will wait as long as the tasks's timeout setting for the
    directory to appear, and will raise a ProjectError otherwise.
    """

    order = 100

    def run(self):
        start = time.time()
        timeout = self.config["timeout"]
        delta = self.config["delta"]
        while not (self.proj.path_proc / "Manual").exists():
            if time.time() - start > timeout:
                raise ProjectError("timeout waiting on manual processing")
            time.sleep(delta)
