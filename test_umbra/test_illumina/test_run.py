"""
Test umbra.illumina.run

More specifically, test the Run class that represents an Illumina run directory
on disk.
"""

from unittest.mock import Mock
import time
import datetime
import warnings
from tempfile import TemporaryDirectory
from pathlib import Path
from distutils.dir_util import copy_tree
from shutil import move
from umbra.illumina.run import Run
from . import test_common
from .test_common import RUN_IDS, PATH_RUNS

class TestRun(test_common.TestBase):
    """Base test case for a Run.

    This is built around a mock MiSeq run directory.
    """

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["MiSeq"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiSeq"]), str(self.path_run))
        self.run = Run(self.path_run)
        date = datetime.datetime(2018, 1, 1, 6, 21, 31, 705000)
        self.expected = {
            "id": RUN_IDS["MiSeq"],
            "cycles": 318,
            "rta": {"Date": date, "Version": "Illumina RTA 1.18.54"},
            "t1": "2018-01-02T06:48:22.0480092-04:00",
            "t2": "2018-01-02T06:48:32.608024-04:00",
            "flowcell": "000000000-XXXXX"
            }

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_attrs(self):
        """Test various Run properties."""
        # RunInfo.xml
        # Check the Run ID.
        id_obs = self.run.run_info.find("Run").attrib["Id"]
        self.assertEqual(id_obs, self.expected["id"])
        # RTAComplete.txt
        # Check the full contents.
        rta_obs = self.run.rta_complete
        self.assertEqual(rta_obs, self.expected["rta"])
        # CompletedJobInfo.xml
        # Check the job start/completion timestamps.
        t1_obs = self.run.completed_job_info.find("StartTime").text
        t2_obs = self.run.completed_job_info.find("CompletionTime").text
        self.assertEqual(self.expected["t1"], t1_obs)
        self.assertEqual(self.expected["t2"], t2_obs)

    def check_refresh_run(self):
        """Test refreshing run state from files on disk."""
        # Starting without a RTAComplete.txt, run is marked incomplete.  We'll
        # also check how the alignment refresh behaves.  It shouldn't call the
        # given callback, if any, until the run itself is complete.  This is an
        # edge case since in theory the run needs to be complete if the
        # alignment is, but I did run into this in a real run directory that
        # was incompletely transferred and this behavior makes the most sense
        # in that situation.
        move(str(self.path_run / "RTAComplete.txt"), str(self.path_run / "tmp.txt"))
        callback = Mock()
        self.run = Run(self.path_run, alignment_callback=callback)
        self.assertFalse(self.run.complete)
        self.assertEqual(self.run.rta_complete, None)
        callback.assert_not_called()
        # It doesn't update automatically.
        move(str(self.path_run / "tmp.txt"), str(self.path_run / "RTAComplete.txt"))
        self.assertFalse(self.run.complete)
        # On refresh, it is now seen as complete.
        self.run.refresh()
        callback.assert_called_once()
        self.assertTrue(self.run.complete)
        self.run.refresh()
        callback.assert_called_once()

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

    def test_load_all_bcl_stats(self):
        """Test loading all of the .stats files into a list."""
        observed = self.run.load_all_bcl_stats()
        expected = []
        for lane in [1]:
            for cycle in range(self.expected["cycles"]):
                for tile in [1101, 1102]:
                    expected.append({
                        'cycle': cycle,
                        'avg_intensity': 0.0,
                        'avg_int_all_A': 0.0,
                        'avg_int_all_C': 0.0,
                        'avg_int_all_G': 0.0,
                        'avg_int_all_T': 0.0,
                        'avg_int_cluster_A': 0.0,
                        'avg_int_cluster_C': 0.0,
                        'avg_int_cluster_G': 0.0,
                        'avg_int_cluster_T': 0.0,
                        'num_clust_call_A': 0,
                        'num_clust_call_C': 0,
                        'num_clust_call_G': 0,
                        'num_clust_call_T': 0,
                        'num_clust_call_X': 0,
                        'num_clust_int_A': 0,
                        'num_clust_int_C': 0,
                        'num_clust_int_G': 0,
                        'num_clust_int_T': 0,
                        'lane': lane,
                        'tile': tile})
            self.assertEqual(observed, expected)

    def test_run_id(self):
        """Test the run ID property."""
        self.assertEqual(self.expected["id"], self.run.run_id)

    def test_flowcell(self):
        """Test the flowcell property."""
        self.assertEqual(self.expected["flowcell"], self.run.flowcell)


class TestRunSingle(TestRun):
    """Test Run with single-ended sequencing."""

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["Single"]
        copy_tree(str(PATH_RUNS / RUN_IDS["Single"]), str(self.path_run))
        self.run = Run(self.path_run)
        self.expected = {}
        self.expected["id"] = RUN_IDS["Single"]
        self.expected["cycles"] = 518
        date = datetime.datetime(2018, 1, 6, 6, 20, 25, 841000)
        self.expected["rta"] = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.expected["t1"] = "2018-01-05T13:38:15.2566992-04:00"
        self.expected["t2"] = "2018-01-05T13:38:45.3021522-04:00"
        self.expected["flowcell"] = "000000000-XXXXX"


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
        self.expected = {}
        self.expected["id"] = RUN_IDS["MiniSeq"]
        date = datetime.datetime(2018, 1, 4, 11, 14, 00)
        self.expected["rta"] = {"Date": date, "Version": "RTA 2.8.6"}
        self.expected["t1"] = "2018-01-04T11:15:03.8237582-04:00"
        self.expected["t2"] = "2018-08-04T11:16:52.4989741-04:00"
        self.expected["flowcell"] = "000000000"

    def test_load_all_bcl_stats(self):
        """Test that a MiniSeq run gives an empty list as it has no BCL stats files."""
        observed = self.run.load_all_bcl_stats()
        self.assertEqual(observed, [])


class TestRunMisnamed(TestRun):
    """Test case for a directory whose name is not the Run ID."""

    def setUp(self):
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["misnamed"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiSeq"]), str(self.path_run))
        self.run = Run(self.path_run, strict=False)
        self.expected = {}
        self.expected["id"] = RUN_IDS["MiSeq"]
        self.expected["cycles"] = 318
        date = datetime.datetime(2018, 1, 1, 6, 21, 31, 705000)
        self.expected["rta"] = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        self.expected["t1"] = "2018-01-02T06:48:22.0480092-04:00"
        self.expected["t2"] = "2018-01-02T06:48:32.608024-04:00"
        self.expected["flowcell"] = "000000000-XXXXX"

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


class TestRunInvalid(test_common.TestBase):
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
