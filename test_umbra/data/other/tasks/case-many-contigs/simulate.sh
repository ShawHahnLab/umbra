#!/usr/bin/env bash

# Special case for many contigs for one sample.

# Put the path to the ART tools in $PATH
for ref in reference*.fasta; do
	art_illumina -ss MSv3 -i $ref -rs 0 -p -l 150 -f 20 -m 200 -s 5 --quiet --noALN -o sample_${ref}_
	sed 's:/1$::' sample_${ref}_1.fq >> sample_S1_L001_R1_001.fastq
	sed 's:/2$::' sample_${ref}_2.fq >> sample_S1_L001_R2_001.fastq
	rm sample_${ref}_{1,2}.fq
done
gzip sample*.fastq
