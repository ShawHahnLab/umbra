"""
Test TaskMergeReads.
"""

import shutil
from pathlib import Path
from umbra import task
from . import test_task

class TestTaskMergeReads(test_task.TestTask):
    """Test TaskMergeReads."""

    def setUp(self):
        # pylint: disable=arguments-differ,no-member
        super().setUp(task.TaskMergeReads)
        # Copy all merge fastq from the test data dir into the temp processing
        # dir
        dir_proc = Path(self.tmpdir.name) / "proc"
        (dir_proc / "trimmed").mkdir(parents=True)
        for fastq in (self.path / "input").glob("*.fastq"):
            shutil.copy(fastq, dir_proc / "trimmed")

    def test_name(self):
        self.assertEqual(self.thing.name, "mergereads")

    def test_source_path(self):
        """Test that source_path is a Path pointing to the source code."""
        self.assertEqual(self.thing.source_path, Path("task_mergereads.py"))

    def test_order(self):
        """Test that the numeric order attribute is defined."""
        self.assertEqual(self.thing.order, 11)

    def test_dependencies(self):
        """Test that the dependency list is defined."""
        self.assertEqual(self.thing.dependencies, ["trim"])

    def test_summary(self):
        """Test the string summary method."""
        summary_expected = [
            "name: mergereads",
            "order: 11",
            "dependencies: trim",
            "source_path: task_mergereads.py"]
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

        We should have a merged fastq for each pair of trimmed fastqs, plus
        PEAR's three other outputs (discarded, unassembled.forward,
        unassembled.reverse).
        """
        outputs = (self.proj.path_proc / "MergedReads").glob("*.fastq")
        outputs = sorted(list(outputs))
        things = ["assembled", "discarded", "unassembled.forward", "unassembled.reverse"]
        output_expected = [f"sample_S1_L001_R_001.{thing}.fastq" for thing in things]
        # Check that we have the expected files
        self.assertEqual([x.name for x in outputs], output_expected)
        # Check that file contents match
        for output in outputs:
            with open(output) as f_observed:
                observed = f_observed.read()
            fp_expected = self.path / "output" / output.name
            with open(fp_expected) as f_expected:
                expected = f_expected.read()
            self.assertEqual(observed, expected)
