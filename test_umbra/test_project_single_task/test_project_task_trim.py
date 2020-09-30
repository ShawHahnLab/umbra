"""
Test for single-task "trim".
"""

from .test_project_task import TestProjectDataOneTask

class TestProjectDataTrim(TestProjectDataOneTask):
    """ Test for single-task "trim".

    Here we should have a set of fastq files in a "trimmed" subdirectory."""

    def set_up_vars(self):
        self.task = "trim"
        super().set_up_vars()

    def test_process(self):
        """Test that the trim task completed as expected."""
        # Let's set up a detailed example in one file pair, to make sure the
        # trimming itself worked.
        seq_pair = ("ACTG" * 10, "CAGT" * 10)
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # We should have a subdirectory with the trimmed files.
        dirpath = self.proj.path_proc / "trimmed"
        # What trimmed files did we observe?
        fastq_obs = [x.name for x in dirpath.glob("*.trimmed.fastq")]
        fastq_obs = sorted(fastq_obs)
        # What trimmed files do we expect for the sample names we have?
        fastq_exp = self.expected_paths()
        # Now, do they match?
        self.assertEqual(fastq_obs, fastq_exp)
        # Was anything else in there?  Shouldn't be.
        files_all = [x.name for x in dirpath.glob("*")]
        files_all = sorted(files_all)
        self.assertEqual(files_all, fastq_exp)
        # Did the specific read pair we created get trimmed as expected?
        pat = str(dirpath / "1086S1-01_S1_L001_R%d_001.trimmed.fastq")
        fps = [pat % d for d in (1, 2)]
        for fp_in, seq_exp in zip(fps, seq_pair):
            with open(fp_in, "r") as f_in:
                seq_obs = f_in.readlines()[1].strip()
                self.assertEqual(seq_obs, seq_exp)
