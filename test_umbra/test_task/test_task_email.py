"""
Test TaskEmail.
"""

from pathlib import Path
from umbra import task
from . import test_task

class TestTaskEmail(test_task.TestTask):
    """Test TaskEmail."""

    def setUp(self):
        # pylint: disable=no-member,arguments-differ
        super().setUp(task.TaskEmail)

    def test_name(self):
        self.assertEqual(self.thing.name, "email")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_email.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 1003)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, ["upload"])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: email",
            "order: 1003",
            "dependencies: upload",
            "source_path: task_email.py"]
        summary_expected = "\n".join(summary_expected)
        self.assertEqual(self.thing.summary, summary_expected)

    def test_run(self):
        self.skipTest("not yet implemented")

    def test_runwrapper(self):
        self.skipTest("not yet implemented")
