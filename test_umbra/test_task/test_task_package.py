"""
Test TaskPackage.
"""

import copy
import tempfile
import unittest
import unittest.mock
from pathlib import Path
from umbra import task
from .test_task import TestTask

class TestTaskPackage(TestTask):
    """Test TaskPackage."""

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
            "log_path": self.proj.path_proc / "logs/log_package.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc
            }
        # pylint: disable=no-member
        self.thing = task.TaskPackage({}, self.proj)

    def test_name(self):
        self.assertEqual(self.thing.name, "package")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_package.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 1001)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, ["metadata"])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: package",
            "order: 1001",
            "dependencies: metadata",
            "source_path: task_package.py"]
        summary_expected = "\n".join(summary_expected)
        self.assertEqual(self.thing.summary, summary_expected)

    def test_run(self):
        self.skipTest("not yet implemented")

    def test_runwrapper(self):
        self.skipTest("not yet implemented")