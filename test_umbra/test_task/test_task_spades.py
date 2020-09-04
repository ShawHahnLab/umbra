"""
Test TaskSpades.
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

class TestTaskSpades(TestTask):
    """Test TaskSpades."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # Copy all merge fastq from the test data dir into the temp processing
        # dir
        dir_input = PATH_DATA / "other" / "tasks" / "task_spades" / "input"
        dir_output = PATH_DATA / "other" / "tasks" / "task_spades" / "output"
        dir_proc = Path(self.tmpdir.name) / "proc"
        dir_proc.mkdir()
        (dir_proc / "PairedReads").mkdir()
        for fastqgz in dir_input.glob("*.fastq"):
            shutil.copy(fastqgz, dir_proc / "PairedReads")
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
            "log_path": self.proj.path_proc / "logs/log_spades.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc,
            "dir_input": dir_input,
            "dir_output": dir_output
            }
        # pylint: disable=no-member
        self.thing = task.TaskSpades({}, self.proj)

    def test_name(self):
        self.assertEqual(self.thing.name, "spades")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_spades.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 12)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, ["merge"])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: spades",
            "order: 12",
            "dependencies: merge",
            "source_path: task_spades.py"]
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

        We should have a contigs fasta for each merged fastq.
        """
        outputs = (self.proj.path_proc / "assembled").glob("*/contigs.fasta")
        outputs = [p.relative_to(self.proj.path_proc / "assembled") for p in outputs]
        outputs = sorted(list(outputs))
        # Check that we have the expected files
        self.assertEqual(
            outputs,
            [Path("sample_S1_L001_R_001/contigs.fasta")])
        # Check that file contents match
        for output in outputs:
            with open(self.proj.path_proc / "assembled" / output) as f_observed:
                observed = f_observed.read()
            fp_expected = self.expected["dir_output"] / output
            with open(fp_expected) as f_expected:
                expected = f_expected.read()
            self.assertEqual(observed, expected)


class TestTaskSpadesManyContigs(TestTaskSpades):
    """Test TaskSpades with reads that should assemble to many contigs."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        # Copy all merge fastq from the test data dir into the temp processing
        # dir
        dir_input = PATH_DATA / "other" / "tasks" / "task_spades" / "input-many-contigs"
        dir_output = PATH_DATA / "other" / "tasks" / "task_spades" / "output-many-contigs"
        dir_proc = Path(self.tmpdir.name) / "proc"
        dir_proc.mkdir()
        (dir_proc / "PairedReads").mkdir()
        for fastqgz in dir_input.glob("*.fastq"):
            shutil.copy(fastqgz, dir_proc / "PairedReads")
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
            "log_path": self.proj.path_proc / "logs/log_spades.txt",
            "sample_paths": copy.deepcopy(self.proj.sample_paths),
            "work_dir_name": "work_dir_name",
            "task_dir_parent": self.proj.path_proc,
            "dir_input": dir_input,
            "dir_output": dir_output
            }
        # pylint: disable=no-member
        self.thing = task.TaskSpades({}, self.proj)
