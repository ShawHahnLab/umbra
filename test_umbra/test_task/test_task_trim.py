"""
Test TaskTrim.
"""

import copy
import tempfile
import shutil
import unittest
import unittest.mock
from pathlib import Path
from umbra import task
from .test_task import TestTask
from ..test_common import PATH_DATA

class TestTaskTrim(TestTask):
    """Test TaskTrim."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # Copy all fastq.gz from the test data dir into the temp processing dir
        dir_input = PATH_DATA / "other" / "tasks" / "task_trim" / "input"
        dir_output = PATH_DATA / "other" / "tasks" / "task_trim" / "output"
        dir_proc = Path(self.tmpdir.name) / "proc"
        dir_proc.mkdir()
        for fastqgz in dir_input.glob("*.fastq.gz"):
            shutil.copy(fastqgz, dir_proc)
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
            "log_path": self.proj.path_proc / "logs/log_trim.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc,
            "dir_input": dir_input,
            "dir_output": dir_output
            }
        # pylint: disable=no-member
        self.thing = task.TaskTrim({}, self.proj)

    def test_name(self):
        self.assertEqual(self.thing.name, "trim")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_trim.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 10)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, [])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: trim",
            "order: 10",
            "dependencies: ",
            "source_path: task_trim.py"]
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

        We should have a trimmed fastq for each original fastq.
        """
        outputs = (self.proj.path_proc / "trimmed").glob("*.trimmed.fastq")
        outputs = sorted(list(outputs))
        # Check that we have the expected files
        self.assertEqual(
            [x.name for x in outputs],
            ["sample_S1_L001_R1_001.trimmed.fastq", "sample_S1_L001_R2_001.trimmed.fastq"])
        # Check that file contents match
        for output in outputs:
            with open(output) as f_observed:
                observed = f_observed.read()
            fp_expected = self.expected["dir_output"] / output.name
            with open(fp_expected) as f_expected:
                expected = f_expected.read()
            self.assertEqual(observed, expected)

class TestTaskTrimManyContigs(TestTaskTrim):
    """Test TaskTrim for reads destined for multiple contigs."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # Copy all fastq.gz from the test data dir into the temp processing dir
        dir_input = PATH_DATA / "other" / "tasks" / "task_trim" / "input-many-contigs"
        dir_output = PATH_DATA / "other" / "tasks" / "task_trim" / "output-many-contigs"
        dir_proc = Path(self.tmpdir.name) / "proc"
        dir_proc.mkdir()
        for fastqgz in dir_input.glob("*.fastq.gz"):
            shutil.copy(fastqgz, dir_proc)
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
            "log_path": self.proj.path_proc / "logs/log_trim.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc,
            "dir_input": dir_input,
            "dir_output": dir_output
            }
        # pylint: disable=no-member
        self.thing = task.TaskTrim({}, self.proj)
