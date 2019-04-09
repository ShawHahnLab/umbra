import unittest
import os
from shutil import move
from tempfile import TemporaryDirectory
from pathlib import Path
from distutils.dir_util import copy_tree
from .test_common import RUN_IDS, PATH_RUNS
from umbra.illumina.run import Alignment

class TestAlignment(unittest.TestCase):
    """Test an Alignment in a typical case (MiSeq, paired-end)."""

    def setUp(self):
        if not hasattr(self, "run_id"):
            self.run_id = RUN_IDS["MiSeq"]
        self.setUpFiles()
        self.setUpVars()
        self.setUpAlignment()

    def setUpVars(self):
        self.num_samples = 35
        self.first_files = [
                "1086S1-01_S1_L001_R1_001.fastq.gz",
                "1086S1-01_S1_L001_R2_001.fastq.gz"
                ]
        self.experiment_exp = "Partials_1_1_18"
        self.path_al = self.path_run / "Data/Intensities/BaseCalls/Alignment"
        self.path_sample_sheet = self.path_al / "SampleSheetUsed.csv"
        self.path_fastq = (self.path_al/ "..").resolve()
        self.path_checkpoint = self.path_al / "Checkpoint.txt"
        self.ss_keys_exp = ["Data", "Header", "Reads", "Settings"]

    def setUpFiles(self):
        # Make a full copy of one run to a temp location
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / self.run_id
        copy_tree(str(PATH_RUNS / self.run_id), str(self.path_run))

    def setUpAlignment(self):
        self.al = Alignment(self.path_al)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_attrs(self):
        """Test attributes from object creation"""
        self.assertEqual(self.al.path_sample_sheet, self.path_sample_sheet)
        self.assertEqual(self.al.path_fastq,        self.path_fastq)
        self.assertEqual(self.al.path_checkpoint,   self.path_checkpoint)
        self.assertEqual(sorted(self.al.sample_sheet.keys()), self.ss_keys_exp)

    def test_index(self):
        """Test that the index within the Run's list of Alignments is correct.
        
        For an Alignment without a Run, there is no index defined.
        """
        self.assertIsNone(self.al.index)

    def test_complete(self):
        """Is an Alignment complete?"""
        # An alignment is complete if Checkpoint exists and is 3, is not
        # complete otherwise.
        self.assertTrue(self.al.complete)
        os.remove(self.al.path_checkpoint)
        self.al = Alignment(self.path_al)
        self.assertFalse(self.al.complete)

    def test_experiment(self):
        """Is the Experiment name available?"""
        self.assertEqual(self.al.experiment, self.experiment_exp)

    def test_sample_numbers(self):
        nums = [i+1 for i in range(self.num_samples)]
        self.assertEqual(self.al.sample_numbers, nums)

    def test_sample_names(self):
        self.assertEqual(len(self.al.sample_names), self.num_samples)

    def test_samples(self):
        data = self.al.sample_sheet["Data"]
        self.assertEqual(self.al.samples, data)

    def test_sample_files_for_num(self):
        """Test expected sample filenames for one sample number"""
        # This run is paired-end so we should get two read files per sample.
        filenames_observed = self.al.sample_files_for_num(1)
        self.assertEqual(filenames_observed, self.first_files)

    def test_sample_paths_for_num(self):
        """Test expected sample paths for one sample number"""
        filepaths_exp = [ self.al.path_fastq / fn for fn in self.first_files]
        filepaths_obs = self.al.sample_paths_for_num(1)
        self.assertEqual(filepaths_obs, filepaths_exp)

    def test_sample_paths(self):
        """Test sample paths by sample name for all samples"""
        sp = self.al.sample_paths()
        # The keys are sample names
        self.assertEqual(sorted(sp.keys()), sorted(self.al.sample_names))
        # The values are sample paths
        v = [self.al.sample_paths_for_num(n) for n in self.al.sample_numbers]
        k = self.al.sample_names
        sp_exp = {k: v for k,v in zip(k, v)}
        self.assertEqual(sp, sp_exp)

    def test_refresh(self):
        """Does refresh catch completion?"""
        # Starting without a Checkpoint.txt, alignment is marked incomplete.
        move(str(self.al.path_checkpoint), str(self.path_run))
        self.al = Alignment(self.path_al)
        self.assertFalse(self.al.complete)
        # It doesn't update automatically.
        move(str(self.path_run / "Checkpoint.txt"), str(self.al.path_checkpoint))
        self.assertFalse(self.al.complete)
        # On refresh, it is now seen as complete.
        self.al.refresh()
        self.assertTrue(self.al.complete)


class TestAlignmentSingleEnded(TestAlignment):
    """Test an Alignment for a non-paired-end Run.
    
    The only thing that should be different here is that the run files will
    only have R1, no R2."""

    def setUp(self):
        self.run_id = RUN_IDS["Single"]
        super().setUp()

    def setUpVars(self):
        super().setUpVars()
        self.num_samples = 4
        self.first_files = ["GA_S1_L001_R1_001.fastq.gz"]
        self.experiment_exp = "ExperimentSingle"


class TestAlignmentFilesMissing(TestAlignment):
    """Test an Alignment when files are missing"""

    def test_sample_paths_for_num(self):
        """Test expected sample paths for one sample number"""
        # By default a missing file throws FileNotFound error.
        filepaths_exp = [ self.al.path_fastq / fn for fn in self.first_files]
        move(str(filepaths_exp[1]), str(self.path_al))
        with self.assertRaises(FileNotFoundError):
            self.al.sample_paths_for_num(1)
        # Unless we give strict = False.  The same names are returned but one
        # of them doesn't exist.
        filepaths_obs = self.al.sample_paths_for_num(1, strict = False)
        self.assertEqual(filepaths_obs, filepaths_exp)

    def test_sample_paths(self):
        """Test sample paths by sample name for all samples"""
        path = self.al.path_fastq / "1086S1-01_S1_L001_R2_001.fastq.gz"
        move(str(path), str(self.path_al))
        with self.assertRaises(FileNotFoundError):
            sp = self.al.sample_paths()
        sp = self.al.sample_paths(strict = False)
        # The keys are sample names
        self.assertEqual(sorted(sp.keys()), sorted(self.al.sample_names))
        # The values are sample paths
        nums = self.al.sample_numbers
        v = [self.al.sample_paths_for_num(n, False) for n in nums]
        k = self.al.sample_names
        sp_exp = {k: v for k,v in zip(k, v)}
        self.assertEqual(sp, sp_exp)


class TestAlignmentMiniSeq(TestAlignment):
    """Test an Alignment from a MiniSeq Run directory."""

    def setUp(self):
        self.run_id = RUN_IDS["MiniSeq"]
        super().setUp()

    def setUpVars(self):
        self.num_samples = 5
        self.first_files = [
                "TL3833-2-3_S1_L001_R1_001.fastq.gz",
                "TL3833-2-3_S1_L001_R2_001.fastq.gz"
                ]
        self.experiment_exp = "MiniSeqExperiment"
        subdir = "20180103_110937"
        self.path_al = self.path_run / "Alignment_1"
        self.path_sample_sheet = self.path_al / subdir / "SampleSheetUsed.csv"
        self.path_fastq = self.path_al / subdir / "Fastq"
        self.path_checkpoint = self.path_al / subdir / "Checkpoint.txt"
        self.ss_keys_exp = ["Data", "Header", "Reads"]


if __name__ == '__main__':
    unittest.main()
