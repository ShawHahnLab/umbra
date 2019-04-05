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
        for samp in self.sample_paths.keys():
            paths = self.sample_paths[samp]
            if len(paths) != 2:
                raise ProjectError("merging needs 2 files per sample")
            fqs_in = [self._get_tp(p) for p in paths]
            fq_out = self.task_path(paths[0],
                                    "merge",
                                    "PairedReads",
                                    ".merged.fastq")
            merge_pair(fq_out, fqs_in)
            # Merge each file pair. If the expected output file is missing,
            # raise an exception.
            if not Path(fq_out).exists():
                msg = "missing output file %s" % fq_out
                raise ProjectError(msg)

    def _get_tp(self, path):
        return self.task_path(path, "trim", "trimmed", ".trimmed.fastq",
                              r1only=False)

def merge_pair(fq_out, fqs_in):
    """Merge reads from the pair of input FASTQs to a single output FASTQ."""
    with open(fq_out, "w") as f_out, \
            open(fqs_in[0], "r") as f_r1, \
            open(fqs_in[1], "r") as f_r2:
        riter1 = SeqIO.parse(f_r1, "fastq")
        riter2 = SeqIO.parse(f_r2, "fastq")
        for rec1, rec2 in zip(riter1, riter2):
            SeqIO.write(rec1, f_out, "fastq")
            SeqIO.write(rec2, f_out, "fastq")
