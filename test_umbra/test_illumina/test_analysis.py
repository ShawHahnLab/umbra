"""
Test umbra.illumina.analysis

More specifically, test the Analysis class and per-instrument-type subclasses
that represent an Analysis (or Alignment, for older instruments) subdirectory
of an Illumina run directory on disk.
"""

from unittest.mock import Mock
from abc import ABC, abstractmethod
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
        self.run = Mock(instrument_type="MiniSeq")
        self.analysis = analysis.AnalysisClassic(
            self.path/"rundir/Alignment_1/20250101_000000", self.run)

    def test_refresh(self):
        self.fail("not yet implemented")

    def test_index(self):
        self.fail("not yet implemented")

    def test_complete(self):
        self.fail("not yet implemented")

    def test_run(self):
        self.assertEqual(
            self.analysis.run,
            self.run)

    def test_path(self):
        self.assertEqual(
            self.analysis.path,
            self.path.resolve()/"rundir/Alignment_1/20250101_000000")

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
        obs = self.analysis.sample_paths_by_name(strict=False)
        self.assertEqual(obs, {})

    def test_sample_paths(self):
        obs = self.analysis.sample_paths(strict=False)
        self.assertEqual(obs, [])
