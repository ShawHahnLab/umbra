"""Prepare for special-case manual processing with Geneious."""

import shutil
import time
from umbra import task
from umbra.util import ProjectError

class TaskGeneious(task.Task):
    """Prepare for special-case manual processing with Geneious.

    The run method will wait as long as the tasks's timeout setting for the
    directory to appear, and will raise a ProjectError otherwise.
    """

    order = 101

    dependencies = ["assemble"]

    def run(self):
        start = time.time()
        timeout = self.config["timeout"]
        delta = self.config["delta"]
        # Wait for a "Geneious" subdirectory to appear in the processing
        # directory.
        # Override the implicit task hiding for this specific case.  This
        # is brittle but should work for the time being.
        paths_move = [
            self.task_dir_parent("merge")/"PairedReads",
            self.task_dir_parent("assemble")/"ContigsGeneious",
            self.task_dir_parent("assemble")/"CombinedGeneious"
            ]
        for path in paths_move:
            if path.parent != self.proj.path_proc:
                shutil.move(str(path), str(self.proj.path_proc))
        while not (self.proj.path_proc / "Geneious").exists():
            if time.time() - start > timeout:
                raise ProjectError("timeout waiting on manual processing")
            time.sleep(delta)
