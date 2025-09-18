"""
Test umbra.illumina.run

More specifically, test the Run class that represents an Illumina run directory
on disk.
"""

from unittest.mock import Mock
import time
import datetime
import warnings
from umbra.illumina.run import Run
from .test_common import dummy_bcl_stats
from ..test_common import TestBase


class TestRun(TestBase):
    """Basic test case for a typical Run."""

    def setUp(self):
        super().setUp()
        self.aln_callback = Mock()
        self.run = Run(
            self.path / "180101_M00000_0000_000000000-XXXXX",
            analysis_callback=self.aln_callback)

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
        analysis directories, and refresh any existing analyses.

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
        expected = dummy_bcl_stats(16, 1)
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

    def test_instrument_type(self):
        """Test the instrument_type property.

        This should be the sequencer model from the run metadata, either parsed
        directly from the XML or inferred indirectly for the older instruments.
        """
        self.assertEqual("MiSeq", self.run.instrument_type)

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
            analysis_callback=self.aln_callback)

    def test_rta_complete(self):
        """Test the rta_complete property.

        For an incomplete run tis doesn't yet exist.
        """
        self.assertEqual(self.run.rta_complete, None)

    def test_complete(self):
        """Test that the run is not reported as complete."""
        self.assertFalse(self.run.complete)

    def test_refresh(self):
        """For an incomplete run the analysis callback should not be called."""
        self.aln_callback.assert_not_called()
        self.run.refresh()
        self.aln_callback.assert_not_called()

class TestRunToComplete(TestBase):
    """Tests for the incomplete -> complete transition.

    Instead of a typical TestRun with all methods and properties tested,
    this checks the behavior of a few key things during the transition from an
    incomplete state to complete.
    """

    def setUp(self):
        super().setUp()
        self.aln_callback = Mock()
        self.run = Run(
            self.path / "180101_M00000_0000_000000000-XXXXX",
            analysis_callback=self.aln_callback)

    def tearDown(self):
        self.reset_complete()

    def make_complete(self):
        """Create a RTAComplete.txt file so the analysis is complete"""
        rta = self.run.path / "RTAComplete.txt"
        with open(rta, "wt") as f_out:
            f_out.write("1/1/2018,06:21:31.705,Illumina RTA 1.18.54\r\n")

    def reset_complete(self):
        """Remove the RTAComplete.txt file, if any."""
        rta = self.run.path / "RTAComplete.txt"
        try:
            rta.unlink()
        except FileNotFoundError:
            pass

    def test_rta_complete(self):
        """Test the rta_complete property.

        This should be a small dictionary parsed parsed from RTAComplete.txt.
        """
        self.assertIsNone(self.run.rta_complete)
        self.make_complete()
        self.assertIsNone(self.run.rta_complete)
        self.run.refresh()
        self.assertEqual(
            self.run.rta_complete,
            {
                "Date": datetime.datetime(2018, 1, 1, 6, 21, 31, 705000),
                "Version": "Illumina RTA 1.18.54"})

    def test_refresh(self):
        """Does run refresh catch completion?

        At first the analysis callback function should not have been called at
        all, whatever state of the analysis.  Only when both the run and
        analysis are complete *and* refresh is called should the callback be
        called.
        """
        self.aln_callback.assert_not_called()
        self.run.refresh()
        self.aln_callback.assert_not_called()
        self.make_complete()
        self.aln_callback.assert_not_called()
        self.run.refresh()
        self.aln_callback.assert_called_once()
        self.run.refresh()
        self.aln_callback.assert_called_once()

    def test_complete(self):
        """Is the Run complete?

        Not until the RTAComplete.txt file arrives and the object is refreshed.
        """
        self.assertFalse(self.run.complete)
        self.make_complete()
        self.assertFalse(self.run.complete)
        self.run.refresh()
        self.assertTrue(self.run.complete)


class TestRunSingle(TestBase):
    """Test Run with single-ended sequencing."""

    def setUp(self):
        super().setUp()
        self.aln_callback = Mock()
        self.run = Run(
            self.path / "180105_M00000_0000_000000000-XXXXX",
            analysis_callback=self.aln_callback)

    def test_run_id(self):
        """Test the run_id str property."""
        self.assertEqual(
            self.run.run_id,
            "180105_M00000_0000_000000000-XXXXX")

    def test_run_info(self):
        """Test the run_info XML object property."""
        self.assertEqual(
            self.run.run_info.find("Run").attrib["Id"],
            "180105_M00000_0000_000000000-XXXXX")

    def test_flowcell(self):
        """Test the flowcell str property."""
        self.assertEqual("000000000-XXXXX", self.run.flowcell)

    def test_rta_complete(self):
        """Test the rta_complete property."""
        self.assertEqual(
            self.run.rta_complete,
            {
                "Date": datetime.datetime(2018, 1, 6, 6, 20, 25, 841000),
                "Version": "Illumina RTA 1.18.54"})

    def test_completed_job_info(self):
        """Test the completed_job_info property."""
        self.assertEqual(
            self.run.completed_job_info.find("StartTime").text,
            "2018-01-05T13:38:15.2566992-04:00")
        self.assertEqual(
            self.run.completed_job_info.find("CompletionTime").text,
            "2018-01-05T13:38:45.3021522-04:00")

    def test_refresh(self):
        """Test the refresh method."""
        self.aln_callback.assert_called_once()
        self.run.refresh()
        self.aln_callback.assert_called_once()

    def test_load_all_bcl_stats(self):
        """Test the load_all_bcl_stats method.

        There are just fewer cycles here with no R2 but otherwise the idea is
        the same as for the paired-end run.
        """
        observed = self.run.load_all_bcl_stats()
        expected = dummy_bcl_stats(12, 1)
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
            analysis_callback=self.aln_callback)

    def test_rta_complete(self):
        """Test the rta_complete property."""
        self.assertEqual(
            self.run.rta_complete,
            {
                "Date": datetime.datetime(2018, 1, 4, 11, 14, 00),
                "Version": "RTA 2.8.6"})

    def test_completed_job_info(self):
        """Test the completed_job_info property."""
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
        """Test the run_id str property."""
        self.assertEqual(
            self.run.run_id,
            "180103_M000000_0000_0000000000")

    def test_run_info(self):
        """Test the run_info XML object property."""
        self.assertEqual(
            self.run.run_info.find("Run").attrib["Id"],
            "180103_M000000_0000_0000000000")

    def test_flowcell(self):
        """Test the flowcell str property."""
        self.assertEqual("000000000", self.run.flowcell)

    def test_load_all_bcl_stats(self):
        """Test that a MiniSeq run gives an empty list as it has no BCL stats files."""
        observed = self.run.load_all_bcl_stats()
        self.assertEqual(observed, [])

    def test_instrument_type(self):
        """Test the instrument_type property, like for MiSeq."""
        self.assertEqual("MiniSeq", self.run.instrument_type)


class TestRunMisnamed(TestRun):
    """Test case for a directory whose name is not the Run ID."""

    def setUp(self):
        TestBase.setUp(self)
        self.path_run = self.path / "run-files-custom-name"
        self.aln_callback = Mock()
        self.run = Run(self.path_run, analysis_callback=self.aln_callback)

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
        path = self.path / "not a run"
        with self.assertRaises(ValueError):
            Run(path)
        with warnings.catch_warnings(record=True) as warn_list:
            warnings.simplefilter("always")
            run = Run(path, strict=False)
            self.assertEqual(1, len(warn_list))
            self.assertTrue(run.invalid)
        path = self.path / "nonexistent"
        with self.assertRaises(ValueError):
            Run(path)
        with warnings.catch_warnings(record=True) as warn_list:
            warnings.simplefilter("always")
            run = Run(path, strict=False)
            self.assertEqual(1, len(warn_list))
            self.assertTrue(run.invalid)


class TestRunMinAnalysisAge(TestBase):
    """Test case for a Run set to ignore too-new analysis directories.

    This should not warn about empty analysis directories if they're newer (by
    ctime) than a certain age."""

    def setUp(self):
        super().setUp()
        self.path_run = self.path / "180101_M00000_0000_000000000-XXXXX"
        self.run = Run(self.path_run)

    def test_init(self):
        """Does Run instantiation respect min_analysis_dir_age?"""
        orig_als = self.run.analyses
        # min_analysis_dir_age should make the Run object skip the "too new"
        # analysis directories.  We'll use a one-second setting for this test,
        # and will touch the directory to reset the ctime.
        self.run.analyses[0].path.touch()
        with self.assertLogs(level="DEBUG") as log_context:
            self.run = Run(self.path_run, min_analysis_dir_age=0.2)
        # That analysis shouldn't have been added to the list yet.  No
        # warnings should have been generated, just a debug log message about
        # the skip.
        self.assertEqual(len(log_context.output), 1)
        self.assertEqual(len(self.run.analyses), len(orig_als)-1)
        # After enough time has passed it should be loaded.
        time.sleep(0.5)
        self.assertEqual(len(self.run.analyses), len(orig_als)-1)
        self.run.refresh()
        self.assertEqual(len(self.run.analyses), len(orig_als))
