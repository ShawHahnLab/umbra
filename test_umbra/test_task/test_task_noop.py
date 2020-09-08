"""
Test TaskNoop.
"""

import copy
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from umbra import task
from .test_task import TestTask

class TestTaskNoop(TestTask):
    """Test TaskNoop."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # set up a mock project object for testing
        self.proj = unittest.mock.Mock(
            path_proc=Path(self.tmpdir.name) / "proc",
            nthreads=1,
            conf={},
            sample_paths={"sample_name": ["R1.fastq.gz", "R2.fastq.gz"]},
            work_dir="work_dir_name",
            experiment_info={"tasks": []}
            )
        # Expected values during tests
        self.expected = {
            "nthreads": 1,
            "log_path": self.proj.path_proc / "logs/log_noop.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc
            }
        # pylint: disable=no-member
        self.thing = task.TaskNoop({}, self.proj)

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
        self.assertTrue(self.expected["log_path"].exists())
        # TODO: actually close the file if an exception does *not* occur.
        # Currently the file is only closed when handling the exception.
        self.assertFalse(self.is_log_open())
        with open(self.expected["log_path"]) as f_in:
            self.assertEqual("", f_in.read())
