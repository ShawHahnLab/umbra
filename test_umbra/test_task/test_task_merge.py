"""
Test TaskMerge.
"""

import shutil
from pathlib import Path
from umbra import task
from . import test_task

class TestTaskMerge(test_task.TestTask):
    """Test TaskMerge."""

    def setUp(self):
        # pylint: disable=no-member,arguments-differ
        super().setUp(task.TaskMerge)
        # Copy test data into the temp processing dir
        dir_proc = Path(self.tmpdir.name) / "proc"
        (dir_proc / "trimmed").mkdir(parents=True)
        for fastq in (self.path / "input").glob("*.fastq"):
            shutil.copy(fastq, dir_proc / "trimmed")

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
            fp_expected = self.path / "output" / output.name
            with open(fp_expected) as f_expected:
                expected = f_expected.read()
            self.assertEqual(observed, expected)


class TestTaskMergeManyContigs(TestTaskMerge):
    """Test TaskMerge with reads destined for multiple contigs.

    Follows the same pattern as TestTaskTrim and TestTaskTrimManyContigs; see
    the supporting files for the different input/output sets by class name.
    """
