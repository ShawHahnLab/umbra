"""
Test for single-task "assemble".
"""

from .test_project_task import TestProjectDataOneTask, DEFAULT_TASKS

class TestProjectDataAssemble(TestProjectDataOneTask):
    """ Test for single-task "assemble".

    This will automatically run the trim/merge/spades tasks, and then
    post-process the contigs: The contigs will be filtered to just those
    greater than a minimum length, renamed to match the sample names, and
    converted to FASTQ for easy combining with the reads.  (This is the
    ContigsGeneious subdirectory.)  Those modified contigs will also be
    concatenated with the original merged reads (CombinedGeneious
    subdirectory).
    """

    def set_up_vars(self):
        self.task = "assemble"
        super().set_up_vars()
        # trim and merge are dependencies of assemble.
        self.expected["tasks"] = ["trim", "merge", "spades", self.task] + DEFAULT_TASKS
        self.expected["task_output"] = {t: {} for t in self.expected["tasks"]}

    def test_process(self):
        """Test that the assemble task completed as expected."""
        seq_pair = ["ACTG" * 10, "CAGT" * 10]
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # We should have one file each in ContigsGeneious and CombinedGeneious
        # per sample.
        dirpath_contigs = self.proj.path_proc / "ContigsGeneious"
        contigs_obs = [x.name for x in dirpath_contigs.glob("*.contigs.fastq")]
        contigs_obs = sorted(contigs_obs)
        dirpath_combo = self.proj.path_proc / "CombinedGeneious"
        combo_obs = [x.name for x in dirpath_combo.glob("*.contigs_reads.fastq")]
        combo_obs = sorted(combo_obs)
        contigs_exp = self.expected_paths(".contigs.fastq", r1only=True)
        combo_exp = self.expected_paths(".contigs_reads.fastq", r1only=True)
        self.assertEqual(contigs_obs, contigs_exp)
        self.assertEqual(combo_obs, combo_exp)
