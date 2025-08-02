"""
Test umbra.illumina.analysis

More specifically, test the Analysis class and per-instrument-type subclasses
that represent an Analysis (or Alignment, for older instruments) subdirectory
of an Illumina run directory on disk.
"""

from unittest.mock import Mock
from abc import ABC, abstractmethod
from tempfile import TemporaryDirectory
from pathlib import Path
from shutil import copytree
from umbra.illumina import analysis
from ..test_common import TestBase

class TestAnalysisInit(TestBase):
    """Test basics of initialization of Analysis objects"""

    def test_init_analysis_obj(self):
        """Test creating an instrument-specific Analysis"""
        with self.assertRaises(analysis.UnrecognizedInstrument):
            analysis.init_analysis_obj("/foo/bar", Mock(instrument_type="Sequencizer9000"))
        for instr_type in ("MiSeq", "MiniSeq", "MiSeqi100Plus", "NextSeq 2000"):
            with self.subTest(instrument_type=instr_type):
                # TODO get an actual test dir, and then confirm it instantiates
                # the right class
                with self.assertRaises(FileNotFoundError):
                    analysis.init_analysis_obj("/foo/bar", Mock(instrument_type=instr_type))

    def test_new_analysis(self):
        """Confirm we can't directly instantiate an Analysis"""
        with self.assertRaisesRegex(TypeError, "abstract class"):
            analysis.Analysis() # pylint: disable=abstract-class-instantiated


def for_all_methods(decorator):
    """Apply decorator to every callable in a class"""
    # https://stackoverflow.com/a/6307868
    def decorate(cls):
        for attr in vars(cls):
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate

@for_all_methods(abstractmethod)
class TestAnalysis(ABC):
    """Framework of tests for any concrete Analysis class (see below)"""

    def test_refresh(self):
        """Test that the refresh method loads the latest data from disk"""

    def test_index(self):
        """Test that the index property gives the index of this Analysis for the run"""

    def test_complete(self):
        """Test that the complete property reports completion of the Analysis"""

    def test_run(self):
        """Test that run property points to the associated Run object"""

    def test_path(self):
        """Test that path property points to Analysis directory path"""

    def test_sample_sheet_path(self):
        """Test that sample_sheet_path property contains full path to sample sheet"""

    def test_sample_sheet(self):
        """Test that sample_sheet property contains parsed sample sheet data"""

    def test_run_name(self):
        """Test that the run_name property points to RunName from the sample sheet"""

    def test_experiment(self):
        """Test that "experiment" is an alias for run_name"""

    def test_sample_paths_by_name(self):
        """Test making a dictionary of sample names to sets of fastq.gz paths"""

    def test_sample_paths(self):
        """Test making a list of sets of fastq.gz paths for each sample"""


class TestAnalysisClassicMiniSeq(TestAnalysis, TestBase):
    """Test AnalysisClassic class for a MiniSeq run's Alignment dir"""

    def setUp(self):
        self.run = Mock(
            instrument_type="MiniSeq",
            analyses=[])
        self.analysis = analysis.AnalysisClassic(
            self.path/"rundir/Alignment_1", self.run)

    def test_refresh(self):
        self.fail("not yet implemented")

    def test_index(self):
        # If the analysis isn't in the run's list yet, assume it's about to be
        # appended as the latest one
        self.assertEqual(self.analysis.index, 0)
        self.run.analyses = ["A", "B", "C"]
        self.assertEqual(self.analysis.index, 3)
        # If it is in the list, just give that index
        self.run.analyses = ["A", self.analysis, "C"]
        self.assertEqual(self.analysis.index, 1)
        # If no run object was given index is just None
        self.assertIsNone(
            analysis.AnalysisClassic(self.path/"rundir/Alignment_1").index)

    def test_complete(self):
        # as the test dir is set up by default, it should show up as complete.
        self.assertTrue(self.analysis.complete)
        with TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            copytree(self.path/"rundir", tmp/"rundir")
            checkpoint_path = tmp/"rundir/Alignment_1/20250101_000000/Checkpoint.txt"
            checkpoint_path.unlink()
            def checkpoint(txt):
                with open(checkpoint_path, "w", encoding="ASCII") as f_out:
                    f_out.write(txt)
            def setup():
                return analysis.AnalysisClassic(tmp/"rundir/Alignment_1", self.run)
            self.assertFalse(
                setup().complete,
                "complete should be False with missing Checkpoint.txt")
            checkpoint("1\r\n\r\n")
            self.assertFalse(
                setup().complete,
                "complete should be False with unexpected content in Checkpoint.txt")
            checkpoint("3\r\n\r\n")
            self.assertTrue(
                setup().complete,
                "complete should be True with expected content in Checkpoint.txt")

    def test_run(self):
        self.assertEqual(
            self.analysis.run,
            self.run)

    def test_path(self):
        self.assertEqual(
            self.analysis.path,
            self.path.resolve()/"rundir/Alignment_1")

    def test_sample_sheet_path(self):
        self.assertEqual(
            self.analysis.sample_sheet_path,
            self.path.resolve()/"rundir/Alignment_1/20250101_000000/SampleSheetUsed.csv")

    def test_sample_sheet(self):
        self.assertEqual(
            self.analysis.sample_sheet["Header"]["Experiment Name"], "MiniSeqTest")

    def test_run_name(self):
        self.assertEqual(self.analysis.run_name, "MiniSeqTest")

    def test_experiment(self):
        self.assertEqual(self.analysis.experiment, "MiniSeqTest")

    def test_sample_paths_by_name(self):
        expected = {
            "sample1": (
                "20250101_000000/Fastq/sample1_S1_L001_R1_001.fastq.gz",
                "20250101_000000/Fastq/sample1_S1_L001_R2_001.fastq.gz"),
            "sample2": (
                "20250101_000000/Fastq/sample2_S2_L001_R1_001.fastq.gz",
                "20250101_000000/Fastq/sample2_S2_L001_R2_001.fastq.gz"),
            "sample3": (
                "20250101_000000/Fastq/sample3_S3_L001_R1_001.fastq.gz",
                "20250101_000000/Fastq/sample3_S3_L001_R2_001.fastq.gz"),
            "sample4": (
                "20250101_000000/Fastq/sample4_S4_L001_R1_001.fastq.gz",
                "20250101_000000/Fastq/sample4_S4_L001_R2_001.fastq.gz")}
        root = self.path.resolve()/"rundir/Alignment_1"
        expected = {key: (root/p[0], root/p[1]) for key, p in expected.items()}
        obs = self.analysis.sample_paths_by_name(strict=False)
        self.assertEqual(obs, expected)

    def test_sample_paths(self):
        expected = [
            ("20250101_000000/Fastq/sample1_S1_L001_R1_001.fastq.gz",
             "20250101_000000/Fastq/sample1_S1_L001_R2_001.fastq.gz"),
            ("20250101_000000/Fastq/sample2_S2_L001_R1_001.fastq.gz",
             "20250101_000000/Fastq/sample2_S2_L001_R2_001.fastq.gz"),
            ("20250101_000000/Fastq/sample3_S3_L001_R1_001.fastq.gz",
             "20250101_000000/Fastq/sample3_S3_L001_R2_001.fastq.gz"),
            ("20250101_000000/Fastq/sample4_S4_L001_R1_001.fastq.gz",
             "20250101_000000/Fastq/sample4_S4_L001_R2_001.fastq.gz")]
        root = self.path.resolve()/"rundir/Alignment_1"
        expected = [(root/p[0], root/p[1]) for p in expected]
        obs = self.analysis.sample_paths(strict=False)
        self.assertEqual(obs, expected)
