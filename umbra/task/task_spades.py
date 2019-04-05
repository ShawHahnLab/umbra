"""Assemble contigs from all samples using SPAdes."""

from pathlib import Path
from subprocess import CalledProcessError
from umbra import task
from umbra.util import touch

class TaskSpades(task.Task):
    """Assemble contigs from all samples.

    This handles de-novo assembly with Spades and some of our own
    post-processing."""

    # pylint: disable=no-member
    order = 12
    dependencies = ["merge"]

    def run(self):
        for samp in self.sample_paths.keys():
            paths = self.sample_paths[samp]
            fq_merged = self.task_path(paths[0],
                                       "merge",
                                       "PairedReads",
                                       ".merged.fastq")
            spades_dir = self.task_path(paths[0], self.name, "assembled")
            self._assemble_reads(fq_merged, spades_dir)

    def _assemble_reads(self, fq_in, dir_out):
        """Assemble a pair of read files with SPAdes.

        This runs spades.py on a single sample, saving the output to a given
        directory.  The contigs, if built, will be in contigs.fasta.  Spades
        seems to crash a lot so if anything goes wrong we just create an empty
        contigs.fasta in the directory and log the error."""
        fp_out = dir_out / "contigs.fasta"
        # Spades always fails for empty input, so we'll explicitly skip that
        # case.  It might crash anyway, so we handle that below too.
        if Path(fq_in).stat().st_size == 0:
            self.logf.write("Skipping assembly for empty file: %s\n" % str(fq_in))
            self.logf.write("creating placeholder contig file.\n")
            touch(fp_out)
            return fp_out
        args = ["spades.py", "--12", fq_in,
                "-o", dir_out,
                "-t", self.nthreads,
                "--phred-offset", self.config["phred_offset"]]
        args = [str(x) for x in args]
        # spades tends to throw nonzero exit codes with short files, empty
        # files, etc.   If something goes wrong during assembly we'll just make
        # a stub file and move on.
        try:
            self.runcmd(args)
        except CalledProcessError:
            self.logf.write("spades exited with errors.\n")
            self.logf.write("creating placeholder contig file.\n")
            touch(fp_out)
        return fp_out
