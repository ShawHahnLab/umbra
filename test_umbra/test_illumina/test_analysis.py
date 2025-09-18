"""
Test umbra.illumina.analysis

More specifically, test the Analysis class and per-instrument-type subclasses
that represent an Analysis (or Alignment, for older instruments) subdirectory
of an Illumina run directory on disk.
"""

from unittest.mock import Mock
from tempfile import TemporaryDirectory
from abc import ABC, abstractmethod
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


class TestAnalysis(ABC):
    """Framework of tests for any concrete Analysis class (see below)"""
    # (I'm trying to avoid repeating the same test code over and over with
    # slight variations in the underlying data, and to make sure I test the
    # same things for each case.  This works, but with the downside that I'm
    # referencing all sorts of methods that don't get defined until the "real"
    # test classes below.)
    #
    # pylint: disable=no-member

    def test_refresh(self):
        """Test that the refresh method loads the latest data from disk

        At first the callback function should not have been called at all for
        an incomplete Analysis.  Only when it's complete *and* refresh is
        called should the callback be called.
        """
        # The Analysis object from setUp() should have already had the
        # completion callback called.
        self.callback.assert_called_once()
        # But what about one that's incomplete to start with?
        callback = Mock()
        with TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            copytree(self.path/self.expected["run_dir"], tmp/self.expected["run_dir"])
            self.reset_complete(tmp)
            analysis2 = type(self.analysis)(tmp/self.expected["dir"], self.run, callback)
            callback.assert_not_called()
            analysis2.refresh()
            callback.assert_not_called()
            self.make_complete(tmp)
            callback.assert_not_called()
            analysis2.refresh()
            callback.assert_called_once()
            analysis2.refresh()
            callback.assert_called_once()

    def test_index(self):
        """Test that the index property gives the index of this Analysis for the run"""
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
            type(self.analysis)(
                self.path/self.expected["dir"]).index)

    def test_complete(self):
        """Test that the complete property reports completion of the Analysis"""
        self.assertTrue(self.analysis.complete)
        with TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            copytree(self.path/self.expected["run_dir"], tmp/self.expected["run_dir"])
            self.reset_complete(tmp)
            def setup():
                return type(self.analysis)(tmp/self.expected["dir"], self.run)
            self.assertFalse(setup().complete)
            self.make_incomplete(tmp)
            self.assertFalse(setup().complete)
            self.make_complete(tmp)
            self.assertTrue(setup().complete)

    @abstractmethod
    def make_complete(self, tmp):
        """Modify a temporary copy of the alignment data to look complete"""

    @abstractmethod
    def make_incomplete(self, tmp):
        """Modify a temporary copy of the alignment data to look still incomplete"""

    @abstractmethod
    def reset_complete(self, tmp):
        """Modify a temporary copy of the alignment data to not yet complete"""

    def test_run(self):
        """Test that run property points to the associated Run object"""
        self.assertEqual(
            self.analysis.run,
            self.run)

    def test_path(self):
        """Test that path property points to Analysis directory path"""
        self.assertEqual(self.analysis.path, self.path.resolve()/self.expected["dir"])

    def test_sample_sheet_path(self):
        """Test that sample_sheet_path property contains full path to sample sheet"""
        self.assertEqual(
            self.analysis.sample_sheet_path,
            self.path.resolve()/self.expected["sample_sheet_path"])

    def test_sample_sheet(self):
        """Test that sample_sheet property contains parsed sample sheet data"""
        self.assertEqual(
            set(self.analysis.sample_sheet) & {"Header", "Reads"},
            {"Header", "Reads"})

    def test_run_name(self):
        """Test that the run_name property points to RunName from the sample sheet"""
        self.assertEqual(self.analysis.run_name, self.expected["run_name"])

    def test_experiment(self):
        """Test that "experiment" is an alias for run_name"""
        self.assertEqual(self.analysis.experiment, self.expected["run_name"])

    def test_sample_paths_by_name(self):
        """Test making a dictionary of sample names to sets of fastq.gz paths"""
        samples = [f"sample{x+1}" for x in range(4)]
        exp = self.expected["sample_paths"]
        root = self.path.resolve()/self.expected["dir"]
        expected = {
            samp: ((root/p[0]).resolve(), (root/p[1]).resolve()) for samp, p in zip(samples, exp)}
        obs = self.analysis.sample_paths_by_name()
        self.assertEqual(obs, expected)

    def test_sample_paths(self):
        """Test making a list of sets of fastq.gz paths for each sample"""
        root = self.path.resolve()/self.expected["dir"]
        expected = [
            ((root/p[0]).resolve(), (root/p[1]).resolve()) for p in self.expected["sample_paths"]]
        obs = self.analysis.sample_paths()
        self.assertEqual(obs, expected)


class TestAnalysisClassic(TestAnalysis):
    """Test AnalysisClassic class, for MiSeq or MiniSeq"""
    # pylint: disable=no-member

    def make_complete(self, tmp):
        checkpoint_path = tmp/self.expected["sub_dir"]/"Checkpoint.txt"
        with open(checkpoint_path, "w", encoding="ASCII") as f_out:
            f_out.write("3\r\n\r\n")

    def make_incomplete(self, tmp):
        checkpoint_path = tmp/self.expected["sub_dir"]/"Checkpoint.txt"
        with open(checkpoint_path, "w", encoding="ASCII") as f_out:
            f_out.write("1\r\n\r\n")

    def reset_complete(self, tmp):
        checkpoint_path = tmp/self.expected["sub_dir"]/"Checkpoint.txt"
        if checkpoint_path.exists():
            checkpoint_path.unlink()


class TestAnalysisClassicMiSeq(TestAnalysisClassic, TestBase):
    """Test AnalysisClassic class for a MiSeq run's Alignment dir"""

    def setUp(self):
        self.run = Mock(
            instrument_type="MiSeq",
            analyses=[])
        self.callback = Mock()
        self.analysis = analysis.AnalysisClassic(
            self.path/"250708_M05588_0825_000000000-GRBN9/Alignment_1", self.run, self.callback)
        self.expected = {
            "run_name": "MiSeqTest",
            "dir": "250708_M05588_0825_000000000-GRBN9/Alignment_1",
            "sub_dir": "250708_M05588_0825_000000000-GRBN9/Alignment_1/20250709_055347",
            "run_dir": "250708_M05588_0825_000000000-GRBN9",
            "sample_sheet_path": "250708_M05588_0825_000000000-GRBN9/Alignment_1/"
                "20250709_055347/SampleSheetUsed.csv",
            "sample_paths": [
            ("20250709_055347/Fastq/sample1_S1_L001_R1_001.fastq.gz",
             "20250709_055347/Fastq/sample1_S1_L001_R2_001.fastq.gz"),
            ("20250709_055347/Fastq/sample2_S2_L001_R1_001.fastq.gz",
             "20250709_055347/Fastq/sample2_S2_L001_R2_001.fastq.gz"),
            ("20250709_055347/Fastq/sample3_S3_L001_R1_001.fastq.gz",
             "20250709_055347/Fastq/sample3_S3_L001_R2_001.fastq.gz"),
            ("20250709_055347/Fastq/sample4_S4_L001_R1_001.fastq.gz",
             "20250709_055347/Fastq/sample4_S4_L001_R2_001.fastq.gz")]}


class TestAnalysisClassicMiSeqOld(TestAnalysisClassic, TestBase):
    """Test AnalysisClassic class for a MiSeq run's Alignment dir, circa 2021

    This tests the Alignment directory output structure from the older Illumina
    software in use on one of our MiSeqs until 2021 or so.  I'm still bothering
    to test that here to make sure we can still process old data if needed.
    """

    def setUp(self):
        self.run = Mock(
            instrument_type="MiSeq",
            analyses=[])
        self.callback = Mock()
        self.analysis = analysis.AnalysisClassic(
            self.path/"210802_M00281_0060_000000000-DCT7T"/
            "Data/Intensities/BaseCalls/Alignment", self.run, self.callback)
        self.expected = {
            "run_name": "MiSeqTestOld",
            "dir": "210802_M00281_0060_000000000-DCT7T/Data/Intensities/BaseCalls/Alignment",
            "sub_dir": "210802_M00281_0060_000000000-DCT7T/Data/Intensities/BaseCalls/Alignment",
            "run_dir": "210802_M00281_0060_000000000-DCT7T",
            "sample_sheet_path": "210802_M00281_0060_000000000-DCT7T/Data/Intensities/"
                "BaseCalls/Alignment/SampleSheetUsed.csv",
            "sample_paths": [
            ("../sample1_S1_L001_R1_001.fastq.gz",
             "../sample1_S1_L001_R2_001.fastq.gz"),
            ("../sample2_S2_L001_R1_001.fastq.gz",
             "../sample2_S2_L001_R2_001.fastq.gz"),
            ("../sample3_S3_L001_R1_001.fastq.gz",
             "../sample3_S3_L001_R2_001.fastq.gz"),
            ("../sample4_S4_L001_R1_001.fastq.gz",
             "../sample4_S4_L001_R2_001.fastq.gz")]}


class TestAnalysisClassicMiniSeq(TestAnalysisClassic, TestBase):
    """Test AnalysisClassic class for a MiniSeq run's Alignment dir"""

    def setUp(self):
        self.run = Mock(
            instrument_type="MiniSeq",
            analyses=[])
        self.callback = Mock()
        self.analysis = analysis.AnalysisClassic(
            self.path/"250606_MN00123_0517_A000H7WW75/Alignment_1", self.run, self.callback)
        self.expected = {
            "run_name": "MiniSeqTest",
            "dir": "250606_MN00123_0517_A000H7WW75/Alignment_1",
            "sub_dir": "250606_MN00123_0517_A000H7WW75/Alignment_1/20250607_055807",
            "run_dir": "250606_MN00123_0517_A000H7WW75",
            "sample_sheet_path": "250606_MN00123_0517_A000H7WW75/Alignment_1/"
                "20250607_055807/SampleSheetUsed.csv",
            "sample_paths": [
            ("20250607_055807/Fastq/sample1_S1_L001_R1_001.fastq.gz",
             "20250607_055807/Fastq/sample1_S1_L001_R2_001.fastq.gz"),
            ("20250607_055807/Fastq/sample2_S2_L001_R1_001.fastq.gz",
             "20250607_055807/Fastq/sample2_S2_L001_R2_001.fastq.gz"),
            ("20250607_055807/Fastq/sample3_S3_L001_R1_001.fastq.gz",
             "20250607_055807/Fastq/sample3_S3_L001_R2_001.fastq.gz"),
            ("20250607_055807/Fastq/sample4_S4_L001_R1_001.fastq.gz",
             "20250607_055807/Fastq/sample4_S4_L001_R2_001.fastq.gz")]}


class TestAnalysisMiSeqi100Plus(TestAnalysis, TestBase):
    """Test AnalysisMiSeqi100Plus class"""

    def setUp(self):
        self.run = Mock(
            instrument_type="MiSeqi100Plus",
            analyses=[])
        self.callback = Mock()
        self.analysis = analysis.AnalysisMiSeqi100Plus(
            self.path/"20250818_SH00364_0003_ASC2128532-SC3/Analysis/1", self.run, self.callback)
        self.expected = {
            "run_name": "MiSeqi100PlusTest",
            "dir": "20250818_SH00364_0003_ASC2128532-SC3/Analysis/1",
            "sub_dir": "20250818_SH00364_0003_ASC2128532-SC3/Analysis/1",
            "run_dir": "20250818_SH00364_0003_ASC2128532-SC3",
            "sample_sheet_path": ("20250818_SH00364_0003_ASC2128532-SC3/Analysis/1/"
                "inputs/SampleSheet.csv"),
            "sample_paths": [
            ("Data/BCLConvert/fastq/sample1_S1_L001_R1_001.fastq.gz",
             "Data/BCLConvert/fastq/sample1_S1_L001_R2_001.fastq.gz"),
            ("Data/BCLConvert/fastq/sample2_S2_L001_R1_001.fastq.gz",
             "Data/BCLConvert/fastq/sample2_S2_L001_R2_001.fastq.gz"),
            ("Data/BCLConvert/fastq/sample3_S3_L001_R1_001.fastq.gz",
             "Data/BCLConvert/fastq/sample3_S3_L001_R2_001.fastq.gz"),
            ("Data/BCLConvert/fastq/sample4_S4_L001_R1_001.fastq.gz",
             "Data/BCLConvert/fastq/sample4_S4_L001_R2_001.fastq.gz")]}

    def make_complete(self, tmp):
        with open(self.path/self.expected["sub_dir"]/
                  "analysisResults.json", encoding="UTF8") as f_in, \
            open(tmp/self.expected["sub_dir"]/
                 "analysisResults.json", "w", encoding="UTF8") as f_out:
            f_out.write(f_in.read())

    def make_incomplete(self, tmp):
        path = tmp/self.expected["sub_dir"]/"analysisResults.json"
        if path.exists():
            path.unlink()

    def reset_complete(self, tmp):
        self.make_incomplete(tmp)


class TestAnalysisNextSeq2000(TestAnalysis, TestBase):
    """Test AnalysisNextSeq2000 class"""

    def setUp(self):
        self.run = Mock(
            instrument_type="NextSeq2000",
            analyses=[])
        self.callback = Mock()
        self.analysis = analysis.AnalysisNextSeq2000(
            self.path/"250304_VH01673_47_2227MWWNX/Analysis/1", self.run, self.callback)
        self.expected = {
            "run_name": "NextSeq2000Test",
            "dir": "250304_VH01673_47_2227MWWNX/Analysis/1",
            "sub_dir": "250304_VH01673_47_2227MWWNX/Analysis/1",
            "run_dir": "250304_VH01673_47_2227MWWNX",
            "sample_sheet_path": ("250304_VH01673_47_2227MWWNX/Analysis/1/"
                "Data/Reports/SampleSheet.csv"),
            "sample_paths": [
            ("Data/fastq/sample1_S1_L001_R1_001.fastq.gz",
             "Data/fastq/sample1_S1_L001_R2_001.fastq.gz"),
            ("Data/fastq/sample2_S2_L001_R1_001.fastq.gz",
             "Data/fastq/sample2_S2_L001_R2_001.fastq.gz"),
            ("Data/fastq/sample3_S3_L001_R1_001.fastq.gz",
             "Data/fastq/sample3_S3_L001_R2_001.fastq.gz"),
            ("Data/fastq/sample4_S4_L001_R1_001.fastq.gz",
             "Data/fastq/sample4_S4_L001_R2_001.fastq.gz")]}

    def make_complete(self, tmp):
        fqcomp_path = tmp/self.expected["sub_dir"]/"Data/fastq/Logs/FastqComplete.txt"
        content = "".join(f"{line}\n" for line in [
            "/opt/edico/bin/dragen Version"
            "2025-03-05T19:12:53Z,Fastq generation complete"])
        with open(fqcomp_path, "w", encoding="ASCII") as f_out:
            f_out.write(content)

    def make_incomplete(self, tmp):
        fqcomp_path = tmp/self.expected["sub_dir"]/"Data/fastq/Logs/FastqComplete.txt"
        content = "".join(f"{line}\n" for line in [
            "/opt/edico/bin/dragen Version"
            "2025-03-05T19:12:53Z,ERR"])
        with open(fqcomp_path, "w", encoding="ASCII") as f_out:
            f_out.write(content)

    def reset_complete(self, tmp):
        fqcomp_path = tmp/self.expected["sub_dir"]/"Data/fastq/Logs/FastqComplete.txt"
        if fqcomp_path.exists():
            fqcomp_path.unlink()
