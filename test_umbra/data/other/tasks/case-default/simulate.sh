#!/usr/bin/env bash

# Make a simulated set of R1/R2 files from a randomized "reference" sequence.
# From here we can gather up the input and output files for each task and store
# them for use during tests.  In general the output of one will be in the input
# of another.

# Put the path to the ART tools in $PATH
art_illumina -ss MSv3 -i reference.fasta -rs 0 -p -l 150 -f 20 -m 200 -s 10 --quiet --noALN -o sample
# The /1 and /2 suffixes are the old-school way of doing it.  Illumina hasn't
# used that for a few years now.
sed 's:/1$::' sample1.fq | gzip > sample_S1_L001_R1_001.fastq.gz && rm sample1.fq
sed 's:/2$::' sample2.fq | gzip > sample_S1_L001_R2_001.fastq.gz && rm sample2.fq
