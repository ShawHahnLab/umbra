"""
Test umbra.illumina.alignment

More specifically, test the Alignment class that represents a specific
subdirectory of an Illumina run directory on disk.
"""

from unittest.mock import Mock
from shutil import move
from umbra.illumina.run import Alignment
from . import test_common

class TestAlignment(test_common.TestBase):
    """Test an Alignment in a typical case (MiSeq, paired-end)."""

    def setUp(self):
        self.path_aln = (
            self.path /
            "180101_M00000_0000_000000000-XXXXX" /
            "Data/Intensities/BaseCalls/Alignment")
        self.aln_callback = Mock()
        self.aln = Alignment(
            self.path_aln, completion_callback=self.aln_callback)

    def test_paths(self):
        """Test the paths dictionary attribute."""
        paths_exp = {
            "checkpoint": "Checkpoint.txt",
            "fastq": "..",
            "job_info": "CompletedJobInfo.xml",
            "sample_sheet": "SampleSheetUsed.csv"}
        paths_exp = {k: (self.path_aln / v).resolve() for k, v in paths_exp.items()}
        self.assertEqual(self.aln.paths, paths_exp)

    def test_index(self):
        """Test that the index within the Run's list of Alignments is correct.

        For an Alignment without a Run, there is no index defined.
        """
        self.assertIsNone(self.aln.index)

    def test_error(self):
        """What's the error message for the alignment?"""
        # In a successful alignment there isn't one.
        self.assertIsNone(self.aln.error)

    def test_complete(self):
        """Is an Alignment complete?"""
        self.assertTrue(self.aln.complete)

    def test_experiment(self):
        """Is the Experiment name available?"""
        self.assertEqual(self.aln.experiment, "Experiment")

    def test_sample_numbers(self):
        """Test for expected number of samples."""
        nums = [i+1 for i in range(4)]
        self.assertEqual(self.aln.sample_numbers, nums)

    def test_sample_names(self):
        """Test existence (not content, currently) of sample names."""
        self.assertEqual(len(self.aln.sample_names), 4)

    def test_samples(self):
        """Test for consistency of sample metadata with sample sheet."""
        self.assertEqual(self.aln.samples, self.aln.sample_sheet["Data"])

    def test_sample_paths_for_num(self):
        """Test expected sample paths for one sample number"""
        first_files = [
            "1086S1-01_S1_L001_R1_001.fastq.gz",
            "1086S1-01_S1_L001_R2_001.fastq.gz"
            ]
        filepaths_exp = [self.aln.paths["fastq"] / fn for fn in first_files]
        filepaths_obs = self.aln.sample_paths_for_num(1)
        self.assertEqual(filepaths_obs, filepaths_exp)

    def test_sample_paths(self):
        """Test sample paths by sample name for all samples"""
        spaths = self.aln.sample_paths()
        # The keys are sample names
        self.assertEqual(
            sorted(spaths.keys()),
            sorted(self.aln.sample_names))
        # The values are sample paths
        vals = [self.aln.sample_paths_for_num(n) for n in self.aln.sample_numbers]
        keys = self.aln.sample_names
        spaths_exp = dict(zip(keys, vals))
        self.assertEqual(spaths, spaths_exp)

    def test_refresh(self):
        """Does refresh catch completion?"""
        self.aln_callback.assert_called_once()
        self.aln.refresh()
        self.aln_callback.assert_called_once()


class TestAlignmentToComplete(test_common.TestBase):
    """Tests for the incomplete -> complete transition."""

    def todo_test_complete(self):
        """Is an Alignment complete?"""
        # An alignment is complete if Checkpoint exists and is 3, is not
        # complete otherwise.
        self.assertTrue(self.aln.complete)
        aln = Alignment(
            self.path / "miseq-no-checkpoint" /
            "180101_M00000_0000_000000000-XXXXX" /
            "Data/Intensities/BaseCalls/Alignment")
        self.assertFalse(aln.complete)

    def todo_test_refresh(self):
        # Starting without a Checkpoint.txt, alignment is marked incomplete.
        self.skipTest("not yet re-implemented")
        # TODO
        move(str(self.aln.paths["checkpoint"]), str(self.paths["run"]))
        self.assertFalse(self.aln.complete)
        # It doesn't update automatically.
        move(
            str(self.paths["run"] / "Checkpoint.txt"),
            str(self.aln.paths["checkpoint"]))
        self.assertFalse(self.aln.complete)
        # On refresh, it is now seen as complete.
        self.aln.refresh()
        self.assertTrue(self.aln.complete)


class TestAlignmentSingleEnded(TestAlignment):
    """Test an Alignment for a non-paired-end Run.

    The only thing that should be different here is that the run files will
    only have R1, no R2."""

    def setUp(self):
        self.path_aln = (
            self.path /
            "180105_M00000_0000_000000000-XXXXX" /
            "Data/Intensities/BaseCalls/Alignment")
        self.aln_callback = Mock()
        self.aln = Alignment(
            self.path_aln, completion_callback=self.aln_callback)

    def test_sample_paths_for_num(self):
        first_files = ["GA_S1_L001_R1_001.fastq.gz"]
        filepaths_exp = [self.aln.paths["fastq"] / fn for fn in first_files]
        filepaths_obs = self.aln.sample_paths_for_num(1)
        self.assertEqual(filepaths_obs, filepaths_exp)

    def test_sample_numbers(self):
        nums = [i+1 for i in range(4)]
        self.assertEqual(self.aln.sample_numbers, nums)

    def test_sample_names(self):
        self.assertEqual(len(self.aln.sample_names), 4)


class TestAlignmentFilesMissing(TestAlignment):
    """Test an Alignment when files are missing"""

    def test_sample_paths_for_num(self):
        # By default a missing file throws FileNotFound error.
        first_files = [
            "1086S1-01_S1_L001_R1_001.fastq.gz"
            ]
        filepaths_exp = [self.aln.paths["fastq"] / fn for fn in first_files]
        with self.assertRaises(FileNotFoundError):
            self.aln.sample_paths_for_num(1)
        # Unless we give strict=False.  The file paths that actually exist are
        # returned, but the R2 file is implicitly missing.
        filepaths_obs = self.aln.sample_paths_for_num(1, strict=False)
        self.assertEqual(filepaths_obs, [filepaths_exp[0]])

    def test_sample_paths(self):
        with self.assertRaises(FileNotFoundError):
            spaths = self.aln.sample_paths()
        spaths = self.aln.sample_paths(strict=False)
        # The keys are sample names
        self.assertEqual(
            sorted(spaths.keys()),
            sorted(self.aln.sample_names))
        # The values are sample paths
        nums = self.aln.sample_numbers
        vals = [self.aln.sample_paths_for_num(n, False) for n in nums]
        keys = self.aln.sample_names
        spaths_exp = dict(zip(keys, vals))
        self.assertEqual(spaths, spaths_exp)


class TestAlignmentMiniSeq(TestAlignment):
    """Test an Alignment from a MiniSeq Run directory."""

    def setUp(self):
        self.path_aln = (
            self.path / "180103_M000000_0000_0000000000" / "Alignment_1")
        self.aln_callback = Mock()
        self.aln = Alignment(
            self.path_aln, completion_callback=self.aln_callback)

    def test_paths(self):
        paths_exp = {
            "checkpoint": "20180103_110937/Checkpoint.txt",
            "fastq": "20180103_110937/Fastq",
            "job_info": "20180103_110937/CompletedJobInfo.xml",
            "sample_sheet": "20180103_110937/SampleSheetUsed.csv"}
        paths_exp = {k: (self.path_aln / v).resolve() for k, v in paths_exp.items()}
        self.assertEqual(self.aln.paths, paths_exp)

    def test_sample_paths_for_num(self):
        first_files = [
            "TL3833-2-3_S1_L001_R1_001.fastq.gz",
            "TL3833-2-3_S1_L001_R2_001.fastq.gz"
            ]
        filepaths_exp = [self.aln.paths["fastq"] / fn for fn in first_files]
        filepaths_obs = self.aln.sample_paths_for_num(1)
        self.assertEqual(filepaths_obs, filepaths_exp)

    def test_sample_numbers(self):
        nums = [i+1 for i in range(5)]
        self.assertEqual(self.aln.sample_numbers, nums)

    def test_sample_names(self):
        self.assertEqual(len(self.aln.sample_names), 5)


class TestAlignmentErrored(TestAlignment):
    """Test an Alignment with an error."""

    def test_error(self):
        """What's the error message for the alignment?"""
        self.assertEqual(self.aln.error, "Whoops")
