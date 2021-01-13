"""
Test TaskCopy.
"""

from pathlib import Path
from umbra import task
from . import test_task

class TestTaskCopy(test_task.TestTask):
    """Test TaskCopy."""

    def setUp(self):
        # pylint: disable=no-member,arguments-differ
        super().setUp(task.TaskCopy)

    def test_name(self):
        self.assertEqual(self.thing.name, "copy")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_copy.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 2)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, [])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: copy",
            "order: 2",
            "dependencies: ",
            "source_path: task_copy.py"]
        summary_expected = "\n".join(summary_expected)
        self.assertEqual(self.thing.summary, summary_expected)

    def test_run(self):
        self.skipTest("not yet implemented")

    def test_runwrapper(self):
        self.skipTest("not yet implemented")
