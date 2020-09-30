"""
Test for single-task "manual".
"""

import threading
from umbra.project import ProjectError
from .test_project_task import TestProjectDataOneTask, DEFAULT_TASKS

class TestProjectDataManual(TestProjectDataOneTask):
    """ Test for single-task "manual".

    Test that a ProjectData with a manual task specified will wait until a
    marker appears and then will continue processing.
    """

    def set_up_vars(self):
        self.task = "manual"
        super().set_up_vars()

    def finish_manual(self):
        """Helper for manual processing test in test_process."""
        (self.proj.path_proc / "Manual").mkdir()

    def test_process(self):
        # It should finish as long as it finds the Manual directory
        timer = threading.Timer(1, self.finish_manual)
        timer.start()
        super().test_process()


class TestProjectDataTimeout(TestProjectDataOneTask):
    """Abstract base for TestProjectDataManualTimeout and GeneiousTimeout below."""

    def set_up_proj(self):
        """Set up project with customized task options.

        We need to override the timing settings to something short enough to
        test here.
        """
        super().set_up_proj()
        task = [task for task in self.proj.tasks if task.name == self.task][0]
        task.config["timeout"] = 1
        task.config["delta"] = 0.5

    def finish_manual(self):
        """Helper for manual processing test in test_process.

        By the time this runs (if ever) processing should already have failed.
        If not, the timeout didn't work, so we'll force processing to complete
        and then the assertion against the expected ProjectData exception in
        check_process will fail.
        """
        if not self.proj.status == self.expected["final_status"]:
            (self.proj.path_proc / self.task.title()).mkdir()

    def check_process(self):
        """Special case for test_process for timed-out manual processing tasks.

        With the timeout defined in set_up_proj the timer we set here should
        NOT trigger in time, and we should get a ProjectError during processing.
        (To handle the failure case we still start the timer, as a fail-safe.)
        """
        timer = threading.Timer(5, self.finish_manual)
        timer.start()
        with self.assertRaisesRegex(ProjectError, "timeout waiting on manual processing"):
            self.proj.process()
        self.assertEqual(self.proj.status, self.expected["final_status"])
        self.assertEqual(self.proj.tasks_pending, DEFAULT_TASKS)
        self.assertEqual(self.proj.tasks_completed, self.expected["tasks_completed"])
        self.assertEqual(self.proj.task_current, self.task)


class TestProjectDataManualTimeout(TestProjectDataTimeout):
    """ Test for single-task "manual" that doesn't finish in time.

    Test that a ProjectData with a manual task specified will wait until a
    marker appears, but will fail after waiting too long.
    """

    def set_up_vars(self):
        self.task = "manual"
        super().set_up_vars()
        self.expected["final_status"] = "failed"
        self.expected["tasks_completed"] = []

    def test_process(self):
        self.check_process()
