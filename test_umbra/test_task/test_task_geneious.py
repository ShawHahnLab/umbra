"""
Test TaskGeneious.
"""

from pathlib import Path
from umbra import task
from . import test_task

class TestTaskGeneious(test_task.TestTask):
    """Test TaskGeneious."""

    def setUp(self):
        # pylint: disable=no-member,arguments-differ
        super().setUp(task.TaskGeneious)

    def test_name(self):
        self.assertEqual(self.thing.name, "geneious")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_geneious.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 101)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, ["assemble"])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: geneious",
            "order: 101",
            "dependencies: assemble",
            "source_path: task_geneious.py"]
        summary_expected = "\n".join(summary_expected)
        self.assertEqual(self.thing.summary, summary_expected)

    def test_run(self):
        self.skipTest("not yet implemented")

    def test_runwrapper(self):
        self.skipTest("not yet implemented")
