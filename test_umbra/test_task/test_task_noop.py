"""
Test TaskNoop.
"""

import unittest
from pathlib import Path
from umbra import task
from . import test_task

class TestTaskNoop(test_task.TestTask):
    """Test TaskNoop."""

    def setUp(self):
        # pylint: disable=no-member,arguments-differ
        super().setUp(task.TaskNoop)

    def test_name(self):
        self.assertEqual(self.thing.name, "noop")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_noop.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 0)

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: noop",
            "order: 0",
            "dependencies: ",
            "source_path: task_noop.py"]
        summary_expected = "\n".join(summary_expected)
        self.assertEqual(self.thing.summary, summary_expected)

    def test_run(self):
        """Test that the run method runs.  Nothing happens, though."""
        self.thing.run()

    @unittest.expectedFailure
    def test_runwrapper(self):
        # We start with no log yet
        self.check_log_setup(before=True)
        # This should quietly do nothing
        self.thing.runwrapper()
        # The log should exist, be closed, and empty.
        self.assertTrue(self.log_path.exists())
        # TODO: actually close the file if an exception does *not* occur.
        # Currently the file is only closed when handling the exception.
        self.assertFalse(self.is_log_open())
        with open(self.log_path) as f_in:
            self.assertEqual("", f_in.read())
