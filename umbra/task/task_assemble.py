"""Assemble contigs from all samples."""

import subprocess
import re
import traceback
from pathlib import Path
from Bio import SeqIO
from umbra import task
from umbra.util import touch

class TaskAssemble(task.Task):
    """Assemble contigs from all samples.

    This handles de-novo assembly with Spades and some of our own
    post-processing."""

    order = 12
    dependencies = ["merge"]

    def run(self):
        with open(self.log_path, "w") as fout:
            try:
                for samp in self.sample_paths.keys():
                    # Set up paths to use
                    paths = self.sample_paths[samp]
                    fq_merged = self.task_path(paths[0],
                                               "merge",
                                               "PairedReads",
                                               ".merged.fastq")
                    fq_contigs = self.task_path(paths[0],
                                                "assemble",
                                                "ContigsGeneious",
                                                ".contigs.fastq")
                    fq_combo = self.task_path(paths[0],
                                              "assemble",
                                              "CombinedGeneious",
                                              ".contigs_reads.fastq")
                    spades_dir = self.task_path(paths[0], "assemble", "assembled")
                    # Assemble and post-process: create FASTQ version for all
                    # contigs above a given length, using altered sequence
                    # descriptions, and then combine with the original reads.
                    fa_contigs = self._assemble_reads(fq_merged, spades_dir, fout)
                    self._prep_contigs_for_geneious(fa_contigs, fq_contigs)
                    _combine_contigs_for_geneious(fq_contigs, fq_merged, fq_combo)
            except Exception as exception:
                msg = traceback.format_exc()
                fout.write(msg + "\n")
                raise exception

    def _assemble_reads(self, fq_in, dir_out, f_log):
        """Assemble a pair of read files with SPAdes.

        This runs spades.py on a single sample, saving the output to a given
        directory.  The contigs, if built, will be in contigs.fasta.  Spades
        seems to crash a lot so if anything goes wrong we just create an empty
        contigs.fasta in the directory and log the error."""
        fp_out = dir_out / "contigs.fasta"
        # Spades always fails for empty input, so we'll explicitly skip that
        # case.  It might crash anyway, so we handle that below too.
        if Path(fq_in).stat().st_size == 0:
            f_log.write("Skipping assembly for empty file: %s\n" % str(fq_in))
            f_log.write("creating placeholder contig file.\n")
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
            subprocess.run(args, stdout=f_log, stderr=f_log, check=True)
        except subprocess.CalledProcessError:
            f_log.write("spades exited with errors.\n")
            f_log.write("creating placeholder contig file.\n")
            touch(fp_out)
        return fp_out

    def _prep_contigs_for_geneious(self, fa_in, fq_out):
        """Filter and format contigs for use in Geneious.

        Keep contigs above a length threshold, and fake quality scores so we
        can get a FASTQ file to combine with the reads in the next step.
        Modify the sequence ID line to be: <sample>-contig_<contig_number>
        """
        match = re.match("(.*)\\.contigs\\.fastq$", fq_out.name)
        sample_prefix = match.group(1)
        with open(fq_out, "w") as f_out, open(fa_in, "r") as f_in:
            for rec in SeqIO.parse(f_in, "fasta"):
                if len(rec.seq) > self.config["contig_length_min"]:
                    rec.letter_annotations["phred_quality"] = [40]*len(rec.seq)
                    match = re.match("^NODE_([0-9])+_.*", rec.id)
                    contig_num = match.group(1)
                    rec.id = "%s-contig_%s" % (sample_prefix, contig_num)
                    rec.description = ""
                    SeqIO.write(rec, f_out, "fastq")


def _combine_contigs_for_geneious(fq_contigs, fq_reads, fq_out):
    """Concatenate formatted contigs and merged reads for Geneious."""
    with open(fq_contigs) as f_contigs, open(fq_reads) as f_reads, open(fq_out, "w") as f_out:
        for line in f_contigs:
            f_out.write(line)
        for line in f_reads:
            f_out.write(line)
