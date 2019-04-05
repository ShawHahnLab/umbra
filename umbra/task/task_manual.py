"""Wait for a "Manual" subdir to appear in the processing directory."""

import time
from umbra import task

class TaskManual(task.Task):
    """Wait for a "Manual" subdir to appear in the processing directory."""

    order = 100

    def run(self):
        while not (self.proj.path_proc / "Manual").exists():
            time.sleep(1)
