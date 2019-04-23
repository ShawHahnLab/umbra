"""
Test umbra.illumina.run

More specifically, test the Run class that represents an Illumina run directory
on disk.
"""

import unittest
import time
import datetime
import warnings
from tempfile import TemporaryDirectory
from pathlib import Path
from distutils.dir_util import copy_tree
from shutil import move
from umbra.illumina.run import Run
from .test_common import RUN_IDS, PATH_RUNS

class TestRun(unittest.TestCase):
    """Base test case for a Run.

    This is built around a mock MiSeq run directory.
    """

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["MiSeq"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiSeq"]), str(self.path_run))
        self.run = Run(self.path_run)
        self.id_exp = RUN_IDS["MiSeq"]
        date = datetime.datetime(2018, 1, 1, 6, 21, 31, 705000)
        self.rta_exp = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.t1_exp = "2018-01-02T06:48:22.0480092-04:00"
        self.t2_exp = "2018-01-02T06:48:32.608024-04:00"

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_attrs(self):
        """Test various Run properties."""
        # RunInfo.xml
        # Check the Run ID.
        id_obs = self.run.run_info.find("Run").attrib["Id"]
        self.assertEqual(id_obs, self.id_exp)
        # RTAComplete.txt
        # Check the full contents.
        rta_obs = self.run.rta_complete
        self.assertEqual(rta_obs, self.rta_exp)
        # CompletedJobInfo.xml
        # Check the job start/completion timestamps.
        t1_obs = self.run.completed_job_info.find("StartTime").text
        t2_obs = self.run.completed_job_info.find("CompletionTime").text
        self.assertEqual(self.t1_exp, t1_obs)
        self.assertEqual(self.t2_exp, t2_obs)

    def check_refresh_run(self):
        """Test refreshing run state from files on disk."""
        # Starting without a RTAComplete.txt, run is marked incomplete.
        move(str(self.path_run / "RTAComplete.txt"), str(self.path_run / "tmp.txt"))
        self.run = Run(self.path_run)
        self.assertFalse(self.run.complete)
        self.assertEqual(self.run.rta_complete, None)
        # It doesn't update automatically.
        move(str(self.path_run / "tmp.txt"), str(self.path_run / "RTAComplete.txt"))
        self.assertFalse(self.run.complete)
        # On refresh, it is now seen as complete.
        self.run.refresh()
        self.assertTrue(self.run.complete)

    def check_refresh_alignments(self):
        """Test refreshing run Alignment directories from files on disk."""
        orig_als = self.run.alignments
        path_checkpoint = self.run.alignments[0].paths["checkpoint"]
        move(str(path_checkpoint), str(self.path_run / "tmp.txt"))
        self.run = Run(self.path_run)
        self.assertEqual(len(self.run.alignments), len(orig_als))
        self.assertFalse(self.run.alignments[0].complete)
        move(str(self.path_run / "tmp.txt"), str(path_checkpoint))
        self.assertFalse(self.run.alignments[0].complete)
        self.run.refresh()
        self.assertTrue(self.run.alignments[0].complete)
        # Now, load any new alignments
        path_al = self.run.alignments[0].path
        move(str(path_al), str(self.path_run / "tmp"))
        self.run = Run(self.path_run)
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        move(str(self.path_run / "tmp"), str(path_al))
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        self.run.refresh()
        self.assertEqual(len(self.run.alignments), len(orig_als))

    def test_refresh(self):
        """Test that a run refresh works"""
        self.check_refresh_run() # 1: Update run completion status
        self.check_refresh_alignments() # 2: refresh existing alignments

    def test_run_id(self):
        """Test the run ID property."""
        self.assertEqual(self.id_exp, self.run.run_id)


class TestRunSingle(TestRun):
    """Test Run with single-ended sequencing."""

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["Single"]
        copy_tree(str(PATH_RUNS / RUN_IDS["Single"]), str(self.path_run))
        self.run = Run(self.path_run)
        self.id_exp = RUN_IDS["Single"]
        date = datetime.datetime(2018, 1, 6, 6, 20, 25, 841000)
        self.rta_exp = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.t1_exp = "2018-01-05T13:38:15.2566992-04:00"
        self.t2_exp = "2018-01-05T13:38:45.3021522-04:00"


class TestRunMiniSeq(TestRun):
    """Like TestRun, but for a MiniSeq run.

    Nothing should really change in how the information is presented, just a
    few details that are different for this particular run.  The class should
    abstract away the actual differences in run directory layout between the
    different sequencer model.s
    """

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["MiniSeq"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiniSeq"]), str(self.path_run))
        self.run = Run(self.path_run)
        self.id_exp = RUN_IDS["MiniSeq"]
        date = datetime.datetime(2018, 1, 4, 11, 14, 00)
        self.rta_exp = {"Date": date, "Version": "RTA 2.8.6"}
        self.t1_exp = "2018-01-04T11:15:03.8237582-04:00"
        self.t2_exp = "2018-08-04T11:16:52.4989741-04:00"


class TestRunMisnamed(TestRun):
    """Test case for a directory whose name is not the Run ID."""

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["misnamed"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiSeq"]), str(self.path_run))
        self.run = Run(self.path_run, strict=False)
        self.id_exp = RUN_IDS["MiSeq"]
        date = datetime.datetime(2018, 1, 1, 6, 21, 31, 705000)
        self.rta_exp = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.t1_exp = "2018-01-02T06:48:22.0480092-04:00"
        self.t2_exp = "2018-01-02T06:48:32.608024-04:00"

    def test_init(self):
        """Test that we get the expected warning during Run initialization."""
        with warnings.catch_warnings(record=True) as warn_list:
            warnings.simplefilter("always")
            Run(self.path_run, strict=True)
            self.assertEqual(1, len(warn_list))
        with warnings.catch_warnings(record=True) as warn_list:
            warnings.simplefilter("always")
            Run(self.path_run)
            self.assertEqual(0, len(warn_list))
        with warnings.catch_warnings(record=True) as warn_list:
            warnings.simplefilter("always")
            Run(self.path_run, strict=False)
            self.assertEqual(0, len(warn_list))


class TestRunInvalid(unittest.TestCase):
    """Test case for a directory that is not an Illumina run."""

    def test_init(self):
        """Test that Run initialization handles invalid an run directory."""
        path = PATH_RUNS / RUN_IDS["not a run"]
        with self.assertRaises(ValueError):
            Run(path)
        with warnings.catch_warnings(record=True) as warn_list:
            warnings.simplefilter("always")
            run = Run(path, strict=False)
            self.assertEqual(1, len(warn_list))
            self.assertTrue(run.invalid)
        path = PATH_RUNS / RUN_IDS["nonexistent"]
        with self.assertRaises(ValueError):
            Run(path)
        with warnings.catch_warnings(record=True) as warn_list:
            warnings.simplefilter("always")
            run = Run(path, strict=False)
            self.assertEqual(1, len(warn_list))
            self.assertTrue(run.invalid)


class TestRunMinAlignmentAge(TestRun):
    """Test case for a Run set to ignore too-new alignment directories.

    This should not warn about empty alignment directories if they're newer (by
    ctime) than a certain age."""

    def check_refresh_alignments(self):
        # We just need to override the alignment refresh check method.  Last
        # time we didn't have Checkpoint.txt, so it didn't consider it
        # complete.  This time we'll see what happens with Alignment
        # directories with no SampleSheetUsed.csv.
        orig_als = self.run.alignments
        # min_alignment_dir_age should make the Run object skip the "too new"
        # alignment directories.  We'll use a one-second setting for this test,
        # and will touch the directory to reset the ctime.
        self.run.alignments[0].path.touch()
        with self.assertLogs(level="DEBUG") as log_context:
            self.run = Run(self.path_run, min_alignment_dir_age=1)
        # That alignment shouldn't have been added to the list yet.  No
        # warnings should have been generated, just a debug log message about
        # the skip.
        self.assertEqual(len(log_context.output), 1)
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        # After enough time has passed it should be loaded.
        time.sleep(1.1)
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        self.run.refresh()
        self.assertEqual(len(self.run.alignments), len(orig_als))
