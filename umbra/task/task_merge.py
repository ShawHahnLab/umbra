"""Interleave forward and reverse reads for each sample."""

from pathlib import Path
from Bio import SeqIO
from umbra import task
from umbra.util import ProjectError

class TaskMerge(task.Task):
    """Interleave forward and reverse reads for each sample."""

    # pylint: disable=no-member
    order = 11
    dependencies = ["trim"]

    def run(self):
        for samp in self.sample_paths:
            paths = self.sample_paths[samp]
            if len(paths) != 2:
                raise ProjectError("merging needs 2 files per sample")
            fqs_in = [self._get_tp(p) for p in paths]
            fq_out = (
                self.task_dir_parent(self.name) /
                "PairedReads" /
                self.read_file_product(paths[0], ".merged.fastq"))
            merge_pair(fq_out, fqs_in)
            # Merge each file pair. If the expected output file is missing,
            # raise an exception.
            if not Path(fq_out).exists():
                msg = "missing output file %s" % fq_out
                raise ProjectError(msg)

    def _get_tp(self, path):
        return (
            self.task_dir_parent("trim") /
            "trimmed" /
            self.read_file_product(path, ".trimmed.fastq", merged=False))

def merge_pair(fq_out, fqs_in):
    """Merge reads from the pair of input FASTQs to a single output FASTQ."""
    task.mkparent(fq_out)
    with open(fq_out, "w") as f_out, \
            open(fqs_in[0], "r") as f_r1, \
            open(fqs_in[1], "r") as f_r2:
        riter1 = SeqIO.parse(f_r1, "fastq")
        riter2 = SeqIO.parse(f_r2, "fastq")
        for rec1, rec2 in zip(riter1, riter2):
            SeqIO.write(rec1, f_out, "fastq")
            SeqIO.write(rec2, f_out, "fastq")
