"""
Test for single-task "merge".
"""

import re
import unittest
from .test_project_task import TestProjectDataOneTask, DEFAULT_TASKS

class TestProjectDataMerge(TestProjectDataOneTask):
    """ Test for single-task "merge".

    Here we should have a set of fastq files in a "PairedReads" subdirectory.
    This will be the interleaved version of the separate trimmed R1/R2 files
    from the trim task."""

    def set_up_vars(self):
        self.task = "merge"
        super().set_up_vars()
        # trim is a dependency of merge.
        self.expected["tasks"] = ["trim", self.task] + DEFAULT_TASKS
        self.expected["task_output"] = {t: {} for t in self.expected["tasks"]}

    def test_process(self):
        """Test that the merge task completed as expected."""
        # Let's set up a detailed example in one file pair, to make sure the
        # merging itself worked (separately testing trimming above).
        seq_pair = ["ACTG" * 10, "CAGT" * 10]
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # We should have a subdirectory with the merged files.
        dirpath = self.proj.path_proc / "PairedReads"
        # What merged files did we observe?
        fastq_obs = [x.name for x in dirpath.glob("*.merged.fastq")]
        fastq_obs = sorted(fastq_obs)
        # What merged files do we expect for the sample names we have?
        fastq_exp = self.expected_paths(".merged.fastq", r1only=True)
        fastq_exp = [re.sub("_R1_", "_R_", p) for p in fastq_exp]
        # Now, do they match?
        self.assertEqual(fastq_obs, fastq_exp)
        # Was anything else in there?  Shouldn't be.
        files_all = [x.name for x in dirpath.glob("*")]
        files_all = sorted(files_all)
        self.assertEqual(files_all, fastq_exp)
        # Did the specific read pair we created get merged as expected?
        # (This isn't super thorough since in this case it's just the same as
        # concatenating the two files.  Maybe add more to prove they're
        # interleaved.)
        fp_in = str(dirpath / "1086S1-01_S1_L001_R_001.merged.fastq")
        with open(fp_in, "r") as f_in:
            data = f_in.readlines()
            seq_obs = [data[i].strip() for i in [1, 5]]
            self.assertEqual(seq_obs, seq_pair)


@unittest.skip("not yet implemented")
class TestProjectDataMergeSingleEnded(TestProjectDataMerge):
    """ Test for single-task "merge" for a singled-ended Run.

    What *should* happen here?  (What does the original trim script do?)
    """
