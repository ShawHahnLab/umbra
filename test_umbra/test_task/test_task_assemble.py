"""
Test TaskAssemble.
"""

from distutils.dir_util import copy_tree
from pathlib import Path
from umbra import task
from . import test_task

class TestTaskAssemble(test_task.TestTask):
    """Test TaskAssemble."""

    def setUp(self):
        # pylint: disable=no-member,arguments-differ
        super().setUp(task.TaskAssemble)
        # Copy all spades-assembled and merged fastq from the test data dir
        # into the temp processing dir
        dir_input = self.path / "input"
        dir_proc = Path(self.tmpdir.name) / "proc"
        copy_tree(str(dir_input), str(dir_proc))

    def test_name(self):
        self.assertEqual(self.thing.name, "assemble")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_assemble.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 13)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, ["spades", "merge"])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: assemble",
            "order: 13",
            "dependencies: spades, merge",
            "source_path: task_assemble.py"]
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

        We should have a ContigsGeneious fastq for each contigs file and a
        CombinedGeneious fastq for each pair of merged read files and
        corresponding contigs file.
        """
        outputs = list((self.proj.path_proc / "ContigsGeneious").glob("*.fastq")) + \
                list((self.proj.path_proc / "CombinedGeneious").glob("*.fastq"))
        outputs = [p.relative_to(self.proj.path_proc) for p in outputs]
        outputs = sorted(list(outputs))
        # Check that we have the expected files
        self.assertEqual(
            outputs,
            [Path("CombinedGeneious/sample_S1_L001_R_001.contigs_reads.fastq"),
             Path("ContigsGeneious/sample_S1_L001_R_001.contigs.fastq")])
        # Check that file contents match
        for output in outputs:
            with open(self.proj.path_proc / output) as f_observed:
                observed = f_observed.read()
            fp_expected = self.path / "output" / output
            with open(fp_expected) as f_expected:
                expected = f_expected.read()
            self.assertEqual(observed, expected)

class TestTaskAssembleManyContigs(TestTaskAssemble):
    """Test TaskAssemble with reads that assembled to many contigs."""
