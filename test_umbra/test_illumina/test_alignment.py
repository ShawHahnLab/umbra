"""
Test umbra.illumina.alignment

More specifically, test the Alignment class that represents a specific
subdirectory of an Illumina run directory on disk.
"""

import unittest
import os
from shutil import move
from tempfile import TemporaryDirectory
from pathlib import Path
from distutils.dir_util import copy_tree
from umbra.illumina.run import Alignment
from .test_common import RUN_IDS, PATH_RUNS

class TestAlignment(unittest.TestCase):
    """Test an Alignment in a typical case (MiSeq, paired-end)."""

    def setUp(self):
        if not hasattr(self, "run_id"):
            self.run_id = RUN_IDS["MiSeq"]
        self.set_up_files()
        self.set_up_vars()
        self.set_up_alignment()

    def set_up_files(self):
        """Make a full copy of one run to a temp location."""
        self.tmpdir = TemporaryDirectory()
        self.paths = {}
        self.paths["run"] = Path(self.tmpdir.name) / self.run_id
        copy_tree(str(PATH_RUNS / self.run_id), str(self.paths["run"]))

    def set_up_vars(self):
        """Initialize paths and expected values for testing."""
        al_path = (
            self.paths["run"] /
            "Data/Intensities/BaseCalls/Alignment")
        self.paths["alignment"] = al_path
        self.expected = {
            "num_samples": 35,
            "first_files": [
                "1086S1-01_S1_L001_R1_001.fastq.gz",
                "1086S1-01_S1_L001_R2_001.fastq.gz"
                ],
            "experiment": "Partials_1_1_18",
            "ss_keys": ["Data", "Header", "Reads", "Settings"],
            "paths": {
                "sample_sheet": al_path / "SampleSheetUsed.csv",
                "fastq": (al_path / "..").resolve(),
                "checkpoint": al_path / "Checkpoint.txt"
                }
            }

    def set_up_alignment(self):
        """Initialize alignment object for testing."""
        self.alignment = Alignment(self.paths["alignment"])

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_attrs(self):
        """Test attributes from object creation"""
        self.assertEqual(
            self.alignment.paths["sample_sheet"],
            self.expected["paths"]["sample_sheet"])
        self.assertEqual(
            self.alignment.paths["fastq"],
            self.expected["paths"]["fastq"])
        self.assertEqual(
            self.alignment.paths["checkpoint"],
            self.expected["paths"]["checkpoint"])
        self.assertEqual(sorted(self.alignment.sample_sheet.keys()), self.expected["ss_keys"])

    def test_index(self):
        """Test that the index within the Run's list of Alignments is correct.

        For an Alignment without a Run, there is no index defined.
        """
        self.assertIsNone(self.alignment.index)

    def test_complete(self):
        """Is an Alignment complete?"""
        # An alignment is complete if Checkpoint exists and is 3, is not
        # complete otherwise.
        self.assertTrue(self.alignment.complete)
        os.remove(self.alignment.paths["checkpoint"])
        alignment = Alignment(self.paths["alignment"])
        self.assertFalse(alignment.complete)

    def test_experiment(self):
        """Is the Experiment name available?"""
        self.assertEqual(self.alignment.experiment, self.expected["experiment"])

    def test_sample_numbers(self):
        """Test for expected number of samples."""
        nums = [i+1 for i in range(self.expected["num_samples"])]
        self.assertEqual(self.alignment.sample_numbers, nums)

    def test_sample_names(self):
        """Test existence (not content, currently) of sample names."""
        self.assertEqual(
            len(self.alignment.sample_names),
            self.expected["num_samples"])

    def test_samples(self):
        """Test for consistency of sample metadata with sample sheet."""
        data = self.alignment.sample_sheet["Data"]
        self.assertEqual(self.alignment.samples, data)

    def test_sample_files_for_num(self):
        """Test expected sample filenames for one sample number"""
        # This run is paired-end so we should get two read files per sample.
        filenames_observed = self.alignment.sample_files_for_num(1)
        self.assertEqual(filenames_observed, self.expected["first_files"])

    def test_sample_paths_for_num(self):
        """Test expected sample paths for one sample number"""
        aln = self.alignment
        first_files = self.expected["first_files"]
        filepaths_exp = [aln.paths["fastq"] / fn for fn in first_files]
        filepaths_obs = aln.sample_paths_for_num(1)
        self.assertEqual(filepaths_obs, filepaths_exp)

    def test_sample_paths(self):
        """Test sample paths by sample name for all samples"""
        aln = self.alignment
        spaths = aln.sample_paths()
        # The keys are sample names
        self.assertEqual(
            sorted(spaths.keys()),
            sorted(aln.sample_names))
        # The values are sample paths
        vals = [aln.sample_paths_for_num(n) for n in aln.sample_numbers]
        keys = aln.sample_names
        spaths_exp = {k: v for k, v in zip(keys, vals)}
        self.assertEqual(spaths, spaths_exp)

    def test_refresh(self):
        """Does refresh catch completion?"""
        # Starting without a Checkpoint.txt, alignment is marked incomplete.
        move(str(self.alignment.paths["checkpoint"]), str(self.paths["run"]))
        alignment = Alignment(self.paths["alignment"])
        self.assertFalse(alignment.complete)
        # It doesn't update automatically.
        move(
            str(self.paths["run"] / "Checkpoint.txt"),
            str(alignment.paths["checkpoint"]))
        self.assertFalse(alignment.complete)
        # On refresh, it is now seen as complete.
        alignment.refresh()
        self.assertTrue(alignment.complete)


class TestAlignmentSingleEnded(TestAlignment):
    """Test an Alignment for a non-paired-end Run.

    The only thing that should be different here is that the run files will
    only have R1, no R2."""

    def setUp(self):
        self.run_id = RUN_IDS["Single"]
        super().setUp()

    def set_up_vars(self):
        super().set_up_vars()
        self.expected["num_samples"] = 4
        self.expected["first_files"] = ["GA_S1_L001_R1_001.fastq.gz"]
        self.expected["experiment"] = "ExperimentSingle"


class TestAlignmentFilesMissing(TestAlignment):
    """Test an Alignment when files are missing"""

    def test_sample_paths_for_num(self):
        """Test expected sample paths for one sample number"""
        # By default a missing file throws FileNotFound error.
        first_files = self.expected["first_files"]
        filepaths_exp = [self.alignment.paths["fastq"] / fn for fn in first_files]
        move(str(filepaths_exp[1]), str(self.paths["alignment"]))
        with self.assertRaises(FileNotFoundError):
            self.alignment.sample_paths_for_num(1)
        # Unless we give strict=False.  The same names are returned but one
        # of them doesn't exist.
        filepaths_obs = self.alignment.sample_paths_for_num(1, strict=False)
        self.assertEqual(filepaths_obs, filepaths_exp)

    def test_sample_paths(self):
        """Test sample paths by sample name for all samples"""
        path = self.alignment.paths["fastq"] / "1086S1-01_S1_L001_R2_001.fastq.gz"
        move(str(path), str(self.paths["alignment"]))
        with self.assertRaises(FileNotFoundError):
            spaths = self.alignment.sample_paths()
        spaths = self.alignment.sample_paths(strict=False)
        # The keys are sample names
        self.assertEqual(
            sorted(spaths.keys()),
            sorted(self.alignment.sample_names))
        # The values are sample paths
        nums = self.alignment.sample_numbers
        vals = [self.alignment.sample_paths_for_num(n, False) for n in nums]
        keys = self.alignment.sample_names
        spaths_exp = {k: v for k, v in zip(keys, vals)}
        self.assertEqual(spaths, spaths_exp)


class TestAlignmentMiniSeq(TestAlignment):
    """Test an Alignment from a MiniSeq Run directory."""

    def setUp(self):
        self.run_id = RUN_IDS["MiniSeq"]
        super().setUp()

    def set_up_vars(self):
        super().set_up_vars()
        self.expected["num_samples"] = 5
        self.expected["first_files"] = [
            "TL3833-2-3_S1_L001_R1_001.fastq.gz",
            "TL3833-2-3_S1_L001_R2_001.fastq.gz"
            ]
        self.expected["experiment"] = "MiniSeqExperiment"
        self.expected["ss_keys"] = ["Data", "Header", "Reads"]
        self.paths["alignment"] = self.paths["run"] / "Alignment_1"
        subpath = self.paths["alignment"] / "20180103_110937"
        self.expected["paths"]["sample_sheet"] = subpath / "SampleSheetUsed.csv"
        self.expected["paths"]["fastq"] = subpath / "Fastq"
        self.expected["paths"]["checkpoint"] = subpath / "Checkpoint.txt"


if __name__ == '__main__':
    unittest.main()
