"""Prepare for special-case manual processing with Geneious."""

import shutil
import time
from umbra import task

class TaskGeneious(task.Task):
    """Prepare for special-case manual processing with Geneious."""

    order = 101

    dependencies = ["assemble"]

    def run(self):
        # Wait for a "Geneious" subdirectory to appear in the processing
        # directory.
        # Override the implicit task hiding for this specific case.  This
        # is brittle but should work for the time being.
        paths_move = [
            self._task_dir_parent("merge")/"PairedReads",
            self._task_dir_parent("assemble")/"ContigsGeneious",
            self._task_dir_parent("assemble")/"CombinedGeneious"
            ]
        for path in paths_move:
            if path.parent != self.proj.path_proc:
                shutil.move(str(path), str(self.proj.path_proc))
        while not (self.proj.path_proc / "Geneious").exists():
            time.sleep(1)
