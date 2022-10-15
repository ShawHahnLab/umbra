"""Merge (assemble) R1/R2 pairs into combined sequences."""

from pathlib import Path
from umbra.util import ProjectError
from umbra import task

class TaskMergeReads(task.Task):
    """Merge (assemble) R1/R2 pairs into combined sequences."""

    # pylint: disable=no-member
    order = 11
    dependencies = ["trim"]

    def run(self):
        for samp in self.sample_paths:
            paths = self.sample_paths[samp]
            if len(paths) != 2:
                raise ProjectError("merging needs 2 files per sample")
            fqs_in = [self._get_tp(p) for p in paths]
            # pear takes an output name prefix for its four output files, not a
            # single filename
            prefix_out = str(
                self.task_dir_parent(self.name) /
                "MergedReads" /
                self.read_file_product(paths[0]))
            self.pear(prefix_out, fqs_in)
            # Merge each file pair. If the expected main output file is
            # missing, raise an exception.
            asm_out = prefix_out + ".assembled.fastq"
            if not Path(asm_out).exists():
                msg = "missing output file %s" % asm_out
                raise ProjectError(msg)

    def _get_tp(self, path):
        return (
            self.task_dir_parent("trim") /
            "trimmed" /
            self.read_file_product(path, ".trimmed.fastq", merged=False))

    def pear(self, prefix_out, fqs_in):
        task.mkparent(prefix_out)
        args = ["pear", "-f", fqs_in[0], "-r", fqs_in[1], "-o", prefix_out]
        self.runcmd(args)
