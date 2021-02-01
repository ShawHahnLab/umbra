"""
Test for single-task "fail".
"""

from umbra.project import ProjectError
from .test_project_task import TestProjectDataOneTask, DEFAULT_TASKS

class TestProjectDataFail(TestProjectDataOneTask):
    """ Test for single-task "fail".

    Here we should see a failure during processing get caught and logged."""

    def set_up_vars(self):
        self.task = "fail"
        super().set_up_vars()
        self.expected["final_status"] = "failed"

    def test_process(self):
        """Test that failure is caught and reported correctly in process()."""
        with self.assertRaises(ProjectError):
            self.proj.process()
        self.assertEqual(self.proj.status, self.expected["final_status"])
        self.assertEqual(self.proj.tasks_pending, DEFAULT_TASKS)
        self.assertEqual(self.proj.tasks_completed, [])
        self.assertEqual(self.proj.task_current, self.task)
