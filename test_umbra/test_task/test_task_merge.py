"""
Test TaskMerge.
"""

import copy
import tempfile
import shutil
import unittest
import unittest.mock
from pathlib import Path
from umbra import task
from . import test_task

class TestTaskMerge(test_task.TestTask):
    """Test TaskMerge."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # Copy all trim fastq from the test data dir into the temp processing
        # dir
        dir_input = self.path / "input"
        dir_output = self.path / "output"
        dir_proc = Path(self.tmpdir.name) / "proc"
        dir_proc.mkdir()
        (dir_proc / "trimmed").mkdir()
        for fastqgz in dir_input.glob("*.fastq"):
            shutil.copy(fastqgz, dir_proc / "trimmed")
        # set up a mock project object for testing
        self.proj = unittest.mock.Mock(
            path_proc=dir_proc,
            nthreads=1,
            conf={},
            sample_paths={
                "sample": [
                    dir_proc/"sample_S1_L001_R1_001.fastq.gz",
                    dir_proc/"sample_S1_L001_R2_001.fastq.gz"]},
            work_dir="work_dir_name",
            experiment_info={"tasks": []}
            )
        # Expected values during tests
        self.expected = {
            "nthreads": 1,
            "log_path": self.proj.path_proc / "logs/log_merge.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc,
            "dir_input": dir_input,
            "dir_output": dir_output
            }
        # pylint: disable=no-member
        self.thing = task.TaskMerge({}, self.proj)

    def test_name(self):
        self.assertEqual(self.thing.name, "merge")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_merge.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 11)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, ["trim"])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: merge",
            "order: 11",
            "dependencies: trim",
            "source_path: task_merge.py"]
        summary_expected = "\n".join(summary_expected)
        self.assertEqual(self.thing.summary, summary_expected)

    def test_run(self):
        self.thing.run()
        self.check_run_results()

    def test_runwrapper(self):
        self.thing.runwrapper()
        self.check_run_results()

    def check_run_results(self):
        """Compare observed file outputs with expected.

        We should have a merged fastq for each pair of trimmed fastqs.
        """
        outputs = (self.proj.path_proc / "PairedReads").glob("*.fastq")
        outputs = sorted(list(outputs))
        # Check that we have the expected files
        self.assertEqual(
            [x.name for x in outputs],
            ["sample_S1_L001_R_001.merged.fastq"])
        # Check that file contents match
        for output in outputs:
            with open(output) as f_observed:
                observed = f_observed.read()
            fp_expected = self.expected["dir_output"] / output.name
            with open(fp_expected) as f_expected:
                expected = f_expected.read()
            self.assertEqual(observed, expected)


class TestTaskMergeManyContigs(TestTaskMerge):
    """Test TaskMerge with reads destined for multiple contigs."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # Copy all trim fastq from the test data dir into the temp processing
        # dir
        dir_input = self.path / "input"
        dir_output = self.path / "output"
        dir_proc = Path(self.tmpdir.name) / "proc"
        dir_proc.mkdir()
        (dir_proc / "trimmed").mkdir()
        for fastqgz in dir_input.glob("*.fastq"):
            shutil.copy(fastqgz, dir_proc / "trimmed")
        # set up a mock project object for testing
        self.proj = unittest.mock.Mock(
            path_proc=dir_proc,
            nthreads=1,
            conf={},
            sample_paths={
                "sample": [
                    dir_proc/"sample_S1_L001_R1_001.fastq.gz",
                    dir_proc/"sample_S1_L001_R2_001.fastq.gz"]},
            work_dir="work_dir_name",
            experiment_info={"tasks": []}
            )
        # Expected values during tests
        self.expected = {
            "nthreads": 1,
            "log_path": self.proj.path_proc / "logs/log_merge.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc,
            "dir_input": dir_input,
            "dir_output": dir_output
            }
        # pylint: disable=no-member
        self.thing = task.TaskMerge({}, self.proj)
