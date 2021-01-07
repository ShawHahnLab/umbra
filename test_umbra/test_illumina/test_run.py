"""
Test umbra.illumina.run

More specifically, test the Run class that represents an Illumina run directory
on disk.
"""

from unittest.mock import Mock
import time
import datetime
import warnings
from shutil import move
from umbra.illumina.run import Run
from .test_common import RUN_IDS, PATH_RUNS, TestBase

def dummy_bcl_stats(cycles, lanes):
    """Build mock bcl stats list with zeros."""
    expected = []
    for lane in range(1, lanes+1):
        for cycle in range(cycles):
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
    return expected


class TestRun(TestBase):
    """Basic test case for a typical Run."""

    def setUp(self):
        super().setUp()
        self.aln_callback = Mock()
        self.run = Run(
            self.path / "180101_M00000_0000_000000000-XXXXX",
            alignment_callback=self.aln_callback)

    def test_rta_complete(self):
        """Test the rta_complete property.

        This should be a small dictionary parsed parsed from RTAComplete.txt.
        """
        self.assertEqual(
            self.run.rta_complete,
            {
                "Date": datetime.datetime(2018, 1, 1, 6, 21, 31, 705000),
                "Version": "Illumina RTA 1.18.54"})

    def test_completed_job_info(self):
        """Test the completed_job_info property.

        This shuld be a data structure parsed from CompletedJobInfo.xml.
        """
        self.assertEqual(
            self.run.completed_job_info.find("StartTime").text,
            "2018-01-02T06:48:22.0480092-04:00")
        self.assertEqual(
            self.run.completed_job_info.find("CompletionTime").text,
            "2018-01-02T06:48:32.608024-04:00")

    def test_refresh(self):
        """Test the refresh method.

        This should refresh the basic run data from disk, load any new
        alignments, and refresh any existing alignments.

        For a complete run, refresh has no effect.
        """
        self.aln_callback.assert_called_once()
        self.run.refresh()
        self.aln_callback.assert_called_once()

    def test_load_all_bcl_stats(self):
        """Test the load_all_bcl_stats method.

        This should load a list of dictionaries, one for each cycle, from the
        .stats files.
        """
        observed = self.run.load_all_bcl_stats()
        expected = dummy_bcl_stats(318, 1)
        self.assertEqual(observed, expected)

    def test_run_id(self):
        """Test the run_id str property.

        This should be the run identifier, usually identical to the directory
        name.
        """
        self.assertEqual(
            self.run.run_id,
            "180101_M00000_0000_000000000-XXXXX")

    def test_run_info(self):
        """Test the run_info XML object property.

        This shuld be a data structure parsed from RunInfo.xml.
        """
        self.assertEqual(
            self.run.run_info.find("Run").attrib["Id"],
            "180101_M00000_0000_000000000-XXXXX")

    def test_flowcell(self):
        """Test the flowcell str property.

        This should be the flowcell identifier as provided by the run metadata.
        """
        self.assertEqual("000000000-XXXXX", self.run.flowcell)

    def test_complete(self):
        """Test the complete property (True for a completed run)."""
        self.assertTrue(self.run.complete)


class TestRunIncomplete(TestRun):
    """Test behavior on an incomplete MiSeq run.

    For the most part the run information will be available but not the
    rta_complete information.
    """

    def setUp(self):
        super().setUp()
        self.aln_callback = Mock()
        self.run = Run(
            self.path / "180101_M00000_0000_000000000-XXXXX",
            alignment_callback=self.aln_callback)

    def test_rta_complete(self):
        """Test the rta_complete property.

        For an incomplete run tis doesn't yet exist.
        """
        self.assertEqual(self.run.rta_complete, None)

    def test_complete(self):
        """Test that the run is not reported as complete."""
        self.assertFalse(self.run.complete)

    def test_refresh(self):
        """For an incomplete run the alignent callback should not be called."""
        self.aln_callback.assert_not_called()
        self.run.refresh()
        self.aln_callback.assert_not_called()


# TODO this should find a new home in a special test case for the incomplete -> complete transition.
#
#    def check_refresh_run(self):
#        # Starting without a RTAComplete.txt, run is marked incomplete.  We'll
#        # also check how the alignment refresh behaves.  It shouldn't call the
#        # given callback, if any, until the run itself is complete.  This is an
#        # edge case since in theory the run needs to be complete if the
#        # alignment is, but I did run into this in a real run directory that
#        # was incompletely transferred and this behavior makes the most sense
#        # in that situation.
#        move(str(self.path_run / "RTAComplete.txt"), str(self.path_run / "tmp.txt"))
#        callback = Mock()
#        self.run = Run(self.path_run, alignment_callback=callback)
#        self.assertFalse(self.run.complete)
#        self.assertEqual(self.run.rta_complete, None)
#        callback.assert_not_called()
#        # It doesn't update automatically.
#        move(str(self.path_run / "tmp.txt"), str(self.path_run / "RTAComplete.txt"))
#        self.assertFalse(self.run.complete)
#        # On refresh, it is now seen as complete.
#        self.run.refresh()
#        callback.assert_called_once()
#        self.assertTrue(self.run.complete)
#        self.run.refresh()
#        callback.assert_called_once()
#
#    def check_refresh_alignments(self):
#        orig_als = self.run.alignments
#        path_checkpoint = self.run.alignments[0].paths["checkpoint"]
#        move(str(path_checkpoint), str(self.path_run / "tmp.txt"))
#        self.run = Run(self.path_run)
#        self.assertEqual(len(self.run.alignments), len(orig_als))
#        self.assertFalse(self.run.alignments[0].complete)
#        move(str(self.path_run / "tmp.txt"), str(path_checkpoint))
#        self.assertFalse(self.run.alignments[0].complete)
#        self.run.refresh()
#        self.assertTrue(self.run.alignments[0].complete)
#        # Now, load any new alignments
#        path_al = self.run.alignments[0].path
#        move(str(path_al), str(self.path_run / "tmp"))
#        self.run = Run(self.path_run)
#        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
#        move(str(self.path_run / "tmp"), str(path_al))
#        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
#        self.run.refresh()
#        self.assertEqual(len(self.run.alignments), len(orig_als))


class TestRunSingle(TestBase):
    """Test Run with single-ended sequencing."""

    def setUp(self):
        super().setUp()
        self.aln_callback = Mock()
        self.run = Run(
            self.path / "180105_M00000_0000_000000000-XXXXX",
            alignment_callback=self.aln_callback)

    def test_run_id(self):
        self.assertEqual(
            self.run.run_id,
            "180105_M00000_0000_000000000-XXXXX")

    def test_run_info(self):
        self.assertEqual(
            self.run.run_info.find("Run").attrib["Id"],
            "180105_M00000_0000_000000000-XXXXX")

    def test_flowcell(self):
        self.assertEqual("000000000-XXXXX", self.run.flowcell)

    def test_rta_complete(self):
        self.assertEqual(
            self.run.rta_complete,
            {
                "Date": datetime.datetime(2018, 1, 6, 6, 20, 25, 841000),
                "Version": "Illumina RTA 1.18.54"})

    def test_completed_job_info(self):
        self.assertEqual(
            self.run.completed_job_info.find("StartTime").text,
            "2018-01-05T13:38:15.2566992-04:00")
        self.assertEqual(
            self.run.completed_job_info.find("CompletionTime").text,
            "2018-01-05T13:38:45.3021522-04:00")

    def test_refresh(self):
        self.aln_callback.assert_called_once()
        self.run.refresh()
        self.aln_callback.assert_called_once()

    def test_load_all_bcl_stats(self):
        observed = self.run.load_all_bcl_stats()
        expected = dummy_bcl_stats(518, 1)
        self.assertEqual(observed, expected)


class TestRunMiniSeq(TestBase):
    """Test MiniSeq run.

    Nothing should really change in how the information is presented, just a
    few details that are different for this particular run.  The class should
    abstract away the actual differences in run directory layout between the
    different sequencer models.
    """

    def setUp(self):
        super().setUp()
        self.aln_callback = Mock()
        self.run = Run(
            self.path / "180103_M000000_0000_0000000000",
            alignment_callback=self.aln_callback)

    def test_rta_complete(self):
        self.assertEqual(
            self.run.rta_complete,
            {
                "Date": datetime.datetime(2018, 1, 4, 11, 14, 00),
                "Version": "RTA 2.8.6"})

    def test_completed_job_info(self):
        self.assertEqual(
            self.run.completed_job_info.find("StartTime").text,
            "2018-01-04T11:15:03.8237582-04:00")
        self.assertEqual(
            self.run.completed_job_info.find("CompletionTime").text,
            "2018-08-04T11:16:52.4989741-04:00")

    def test_refresh(self):
        """Test that for a complete run, refresh has no effect."""
        self.aln_callback.assert_called_once()
        self.run.refresh()
        self.aln_callback.assert_called_once()

    def test_run_id(self):
        self.assertEqual(
            self.run.run_id,
            "180103_M000000_0000_0000000000")

    def test_run_info(self):
        self.assertEqual(
            self.run.run_info.find("Run").attrib["Id"],
            "180103_M000000_0000_0000000000")

    def test_flowcell(self):
        self.assertEqual("000000000", self.run.flowcell)

    def test_load_all_bcl_stats(self):
        """Test that a MiniSeq run gives an empty list as it has no BCL stats files."""
        observed = self.run.load_all_bcl_stats()
        self.assertEqual(observed, [])


class TestRunMisnamed(TestRun):
    """Test case for a directory whose name is not the Run ID."""

    def setUp(self):
        TestBase.setUp(self)
        self.path_run = self.path / "run-files-custom-name"
        self.aln_callback = Mock()
        self.run = Run(self.path_run, alignment_callback=self.aln_callback)

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


class TestRunInvalid(TestBase):
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


class TestRunMinAlignmentAge(TestBase):
    """Test case for a Run set to ignore too-new alignment directories.

    This should not warn about empty alignment directories if they're newer (by
    ctime) than a certain age."""

    # TODO this will no longer work with the new static file approach for most
    # tests.  Needs to be rebuilt.
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
