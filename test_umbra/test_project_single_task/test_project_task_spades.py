"""
Test for single-task "spades".
"""

from .test_project_task import TestProjectDataOneTask, DEFAULT_TASKS

class TestProjectDataSpades(TestProjectDataOneTask):
    """Test for single-task "spades".

    This will automatically run the trim and merge tasks, and then build
    contigs de-novo from the reads with SPAdes.
    """

    def set_up_vars(self):
        self.task = "spades"
        super().set_up_vars()
        # trim and merge are dependencies of assemble.
        self.expected["tasks"] = ["trim", "merge", self.task] + DEFAULT_TASKS
        self.expected["task_output"] = {t: {} for t in self.expected["tasks"]}

    def test_process(self):
        """Test that the spades task completed as expected."""
        seq_pair = ["ACTG" * 10, "CAGT" * 10]
        self.fake_fastq(seq_pair)
        # The basics
        super().test_process()
        # TODO
        # Next, check that we have the output we expect from spades.  Ideally
        # we should have a true test but right now we get no contigs built.
