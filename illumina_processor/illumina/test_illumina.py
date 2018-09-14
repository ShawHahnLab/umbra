#!/usr/bin/env python
import unittest
import warnings
import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from distutils.dir_util import copy_tree, remove_tree, mkpath
from distutils.file_util import copy_file
from shutil import move
import os

# TODO fix this
import sys
sys.path.append(str((Path(__file__).parent/"..").resolve()))
from illumina.run import Run, Alignment

RUN_IDS = {
        "MiSeq":       "180101_M00000_0000_000000000-XXXXX",
        "MiniSeq":     "180103_M000000_0000_0000000000",
        "Single":      "180105_M00000_0000_000000000-XXXXX",
        "misnamed":    "run-files-custom-name",
        "not a run":   "something_else",
        "nonexistent": "fictional directory"
        }

PATH_ROOT = Path(__file__).parent / "testdata"
PATH_RUNS = PATH_ROOT / "runs"

class TestRun(unittest.TestCase):
    """Base test case for a Run."""

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

    def test_refresh(self):
        """Test that a run refresh works"""
        ### 1: Update run completion status
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
        orig_als = self.run.alignments
        ### 2: refresh existing alignments
        path_checkpoint = self.run.alignments[0].path_checkpoint
        move(str(path_checkpoint), str(self.path_run / "tmp.txt"))
        self.run = Run(self.path_run)
        self.assertEqual(len(self.run.alignments), len(orig_als))
        self.assertFalse(self.run.alignments[0].complete)
        move(str(self.path_run / "tmp.txt"), str(path_checkpoint))
        self.assertFalse(self.run.alignments[0].complete)
        self.run.refresh()
        self.assertTrue(self.run.alignments[0].complete)
        ### 3: load any new alignments
        path_al = self.run.alignments[0].path
        move(str(path_al), str(self.path_run / "tmp"))
        self.run = Run(self.path_run)
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        move(str(self.path_run / "tmp"), str(path_al))
        self.assertEqual(len(self.run.alignments), len(orig_als)-1)
        self.run.refresh()
        self.assertEqual(len(self.run.alignments), len(orig_als))

    def test_run_id(self):
        self.assertEqual(self.id_exp, self.run.run_id)


class TestRunSingle(TestRun):

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

    def test_init(self):
        path = PATH_RUNS / RUN_IDS["misnamed"]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Run(path)
            self.assertEqual(1, len(w))


class TestRunInvalid(unittest.TestCase):
    """Test case for a directory that is not an Illumina run."""

    def test_init(self):
        path = PATH_RUNS / RUN_IDS["not a run"]
        with self.assertRaises(ValueError):
            Run(path)
        path = PATH_RUNS / RUN_IDS["nonexistent"]
        with self.assertRaises(ValueError):
            Run(path)


class TestAlignment(unittest.TestCase):

    def setUp(self):
        self.num_samples = 35
        self.first_files = [
                "1086S1-01_S1_L001_R1_001.fastq.gz",
                "1086S1-01_S1_L001_R2_001.fastq.gz"
                ]
        self.experiment_exp = "Partials_1_1_18"
        # Make a full copy of one run to a temp location
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["MiSeq"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiSeq"]), str(self.path_run))
        # Create an Alignment using the temp files
        self.path_al = self.path_run / "Data/Intensities/BaseCalls/Alignment"
        self.al = Alignment(self.path_al)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_attrs(self):
        """Test attributes from object creation"""
        self.assertEqual(self.al.path_sample_sheet, self.path_al / "SampleSheetUsed.csv")
        self.assertEqual(self.al.path_fastq,        (self.path_al/ "..").resolve())
        self.assertEqual(self.al.path_checkpoint,   self.path_al / "Checkpoint.txt")
        keys_exp = ["Data", "Header", "Reads", "Settings"]
        self.assertEqual(sorted(self.al.sample_sheet.keys()), keys_exp)
        self.assertEqual(self.al.checkpoint, 3)

    def test_index(self):
        self.fail("test not yet implemented")

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
        self.assertEqual(self.al.checkpoint, None)
        # It doesn't update automatically.
        move(str(self.path_run / "Checkpoint.txt"), str(self.al.path_checkpoint))
        self.assertFalse(self.al.complete)
        # On refresh, it is now seen as complete.
        self.al.refresh()
        self.assertTrue(self.al.complete)
        self.assertEqual(self.al.checkpoint, 3)


class TestAlignmentSingleEnded(TestAlignment):
    """Test an Alignment for a non-paired-end Run.
    
    The only thing that should be different here is that the run files will
    only have R1, no R2."""

    def setUp(self):
        self.num_samples = 4
        self.first_files = ["GA_S1_L001_R1_001.fastq.gz"]
        self.experiment_exp = "ExperimentSingle"
        # Make a full copy of one run to a temp location
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["Single"]
        copy_tree(str(PATH_RUNS / RUN_IDS["Single"]), str(self.path_run))
        # Create an Alignment using the temp files
        self.path_al = self.path_run / "Data/Intensities/BaseCalls/Alignment"
        self.al = Alignment(self.path_al)


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
        self.num_samples = 5
        self.first_files = [
                "TL3833-2-3_S1_L001_R1_001.fastq.gz",
                "TL3833-2-3_S1_L001_R2_001.fastq.gz"
                ]
        # Make a full copy of one run to a temp location
        self.tmpdir = TemporaryDirectory()
        self.path_run = Path(self.tmpdir.name) / RUN_IDS["MiniSeq"]
        copy_tree(str(PATH_RUNS / RUN_IDS["MiniSeq"]), str(self.path_run))
        # Create an Alignment using the temp files
        self.path_al = self.path_run / "Alignment_1"
        self.al = Alignment(self.path_al)

    def test_attrs(self):
        """Test attributes from object creation"""
        # Here the file paths should be different compared with MiSeq
        subdir = self.al.path_fastq.parent
        self.assertEqual(self.al.path_sample_sheet, self.path_al / subdir / "SampleSheetUsed.csv")
        self.assertEqual(self.al.path_fastq,        self.path_al / subdir / "Fastq")
        self.assertEqual(self.al.path_checkpoint,   self.path_al / subdir / "Checkpoint.txt")
        keys_exp = ["Data", "Header", "Reads"]
        self.assertEqual(sorted(self.al.sample_sheet.keys()), keys_exp)
        self.assertEqual(self.al.checkpoint, 3)

    def test_experiment(self):
        """Is the Experiment name available?"""
        self.assertEqual(self.al.experiment, "MiniSeqExperiment")

    def test_sample_files_for_num(self):
        """Test expected sample filenames for one sample number"""
        # This run is paired-end so we should get two read files per sample.
        filenames_observed = self.al.sample_files_for_num(1)
        self.assertEqual(filenames_observed, self.first_files)


if __name__ == '__main__':
    unittest.main()
