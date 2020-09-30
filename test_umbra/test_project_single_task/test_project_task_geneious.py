"""
Test for single-task "geneious".
"""

import threading
from .test_project_task import TestProjectDataOneTask, DEFAULT_TASKS
from .test_project_task_manual import TestProjectDataTimeout

class TestProjectDataGeneious(TestProjectDataOneTask):
    """ Test for single-task "geneious".

    Test that a ProjectData with a geneious task specified will wait until a
    marker appears and then will continue processing.
    """

    def set_up_vars(self):
        self.task = "geneious"
        super().set_up_vars()
        # We have a special case here where we want to always see some task
        # dirs at the top level.  Note, this *should* be changed to be handled
        # via the always_explicit config option (but this is not yet done).
        self.config = {
            "implicit_tasks_path": "RunDiagnostics/ImplicitTasks"
            }
        self.expected["tasks"] = ["trim", "merge", "spades", "assemble",
                                  "geneious"] + DEFAULT_TASKS
        self.expected["task_output"] = {t: {} for t in self.expected["tasks"]}

    def finish_manual(self):
        """Helper for manual processing test in test_process."""
        (self.proj.path_proc / "Geneious").mkdir()

    def test_process(self):
        # It should finish as long as it finds the Geneious directory
        timer = threading.Timer(1, self.finish_manual)
        timer.start()
        super().test_process()
        # Despite the config, these directories should now be at the top level.
        self.assertTrue((self.proj.path_proc / "PairedReads").exists())
        self.assertTrue((self.proj.path_proc / "ContigsGeneious").exists())
        self.assertTrue((self.proj.path_proc / "CombinedGeneious").exists())


class TestProjectDataGeneiousTimeout(TestProjectDataTimeout):
    """ Test for single-task "geneious" that doesn't finish in time.

    Test that a ProjectData with a geneious task specified will wait until a
    marker appears, but will fail after waiting too long.
    """

    def set_up_vars(self):
        self.task = "geneious"
        super().set_up_vars()
        self.expected["final_status"] = "failed"
        tasks = ["trim", "merge", "spades", "assemble"]
        self.expected["tasks_completed"] = tasks
        self.expected["tasks"] = tasks + ["geneious"] + DEFAULT_TASKS
        self.expected["task_output"] = {t: {} for t in tasks}

    def test_process(self):
        self.check_process()

    def test_task_output(self):
        self.test_process()
        self.assertEqual(
            sorted(self.proj.task_output.keys()),
            sorted(self.expected["task_output"].keys()))
