"""Custom post-assembly task for all samples."""

import re
from Bio import SeqIO
from umbra import task

class TaskAssemble(task.Task):
    """Assemble contigs from all samples.

    This handles de-novo assembly with Spades and some of our own
    post-processing."""

    order = 13
    dependencies = ["spades", "merge"]

    def run(self):
        for samp in self.sample_paths.keys():
            # Set up paths to use
            paths = self.sample_paths[samp]
            fq_merged = self.task_path(paths[0],
                                       "merge",
                                       "PairedReads",
                                       ".merged.fastq")
            fa_contigs = self.task_path(paths[0],
                                        "spades",
                                        "assembled") / "contigs.fasta"
            fq_contigs = self.task_path(paths[0],
                                        self.name,
                                        "ContigsGeneious",
                                        ".contigs.fastq")
            fq_combo = self.task_path(paths[0],
                                      self.name,
                                      "CombinedGeneious",
                                      ".contigs_reads.fastq")
            # Post-process the assembled contigs: create FASTQ version for all
            # contigs above a given length, using altered sequence
            # descriptions, and then combine with the original reads.
            self._prep_contigs_for_geneious(fa_contigs, fq_contigs)
            _combine_contigs_for_geneious(fq_contigs, fq_merged, fq_combo)

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
