"""
Test TaskSpades.

This has both fast, isolated unit testing of the task code using a fake
spades.py script and an alternate set of tests with the real thing.  By default
the fast tests are run but set CONFIG["live"] to true to switch to testing with
the real spades script.
"""

import unittest
import os
import shutil
from pathlib import Path
from umbra import task
from . import test_task
from ..test_common import CONFIG

class TestTaskSpades(test_task.TestTask):
    """Test TaskSpades."""

    def setUp(self):
        # pylint: disable=no-member,arguments-differ
        #if CONFIG.get("live"):
        #    self.skipTest("skipping mock case; live testing instead")
        super().setUp(task.TaskSpades)
        # Copy all merge fastq from the test data dir into the temp processing
        # dir
        dir_proc = Path(self.tmpdir.name) / "proc"
        (dir_proc / "PairedReads").mkdir(parents=True)
        for fastq in (self.path / "input").glob("*.fastq"):
            shutil.copy(fastq, dir_proc / "PairedReads")
        # Set up mock script via environment variables
        self.set_up_env()

    def tearDown(self):
        super().tearDown()
        self.tear_down_env()

    def set_up_env(self):
        """Prepend dir to PATH and set up test env vars."""
        self.path_orig = os.environ["PATH"]
        os.environ["PATH"] = "%s%s%s" % (
            (self.path / "bin"), os.pathsep, os.environ["PATH"])
        os.environ["TEST_TMPDIR"] = self.tmpdir.name
        os.environ["TEST_LOG"] = self.tmpdir.name + "/.log"

    def tear_down_env(self):
        """Restore the original PATH variable and remove temp env vars."""
        os.environ["PATH"] = self.path_orig
        try:
            del os.environ["TEST_TMPDIR"]
        except KeyError:
            pass
        try:
            del os.environ["TEST_LOG"]
        except KeyError:
            pass

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
        """Check that spades.py was called as expected."""
        with open(os.environ["TEST_LOG"]) as log:
            args_observed = [line.strip() for line in log]
        args_expected = ["--12",
            "$TEST_TMPDIR/proc/PairedReads/sample_S1_L001_R_001.merged.fastq",
            "-o",
            "$TEST_TMPDIR/proc/assembled/sample_S1_L001_R_001",
            "-t",
            "1",
            "--phred-offset",
            "33"]
        self.assertEqual(args_observed, args_expected)


class TestTaskSpadesLive(TestTaskSpades):
    """Like TestTaskSpades but using real spades.

    This is slow for common use and isn't as clean of a test case as
    TestTaskSpades (which uses a mock spades.py) but provides a "live" test
    with actual assembly.
    """

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
            fp_expected = self.path / "output" / output
            with open(fp_expected) as f_expected:
                expected = f_expected.read()
            self.assertEqual(observed, expected)


class TestTaskSpadesLiveManyContigs(TestTaskSpadesLive):
    """Test TaskSpades with reads that should assemble to many contigs.

    Follows the same pattern as TestTaskTrim and TestTaskTrimManyContigs; see
    the supporting files for the different input/output sets by class name.
    """


# Conditionally decorate one set of test cases or the other depending on the
# config setting for running the real spades.py
if CONFIG.get("live"):
    TestTaskSpades = unittest.skip(TestTaskSpades)
else:
    TestTaskSpadesLive = unittest.skip(TestTaskSpadesLive)
    TestTaskSpadesLiveManyContigs = unittest.skip(TestTaskSpadesLiveManyContigs)
