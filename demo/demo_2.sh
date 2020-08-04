#!/usr/bin/env bash

# A specific test with more complicated path configuration.
# For #44.

source demo/demo_setup.sh

SEQROOT=${1-demo_2_seq}
CONFIG=${2-demo/demo_2.yml}

function run_demo_2 {
	rsync -r --exclude STR.yml test_umbra/data/ $SEQROOT
	run_umbra "$SEQROOT" "$CONFIG"
}

# Check that we get exactly the expected files.
function test_files {
	local dir_work=$SEQROOT/processed/2018-01-01-STR-Jesse-XXXXX
	diff <((
	for task in trim metadata package upload email; do
		echo "$dir_work/RunDiagnostics/Logs/log_${task}.txt"
	done
	for filename in SampleSheetUsed.csv STR.yml metadata.csv; do
		echo "$dir_work/RunDiagnostics/ImplicitTasks/Metadata/$filename"
	done
	for prefix in 1086S1-01_S1 1086S1-02_S2; do
		echo "$dir_work/trimmed/${prefix}_L001_R1_001.trimmed.fastq"
		echo "$dir_work/trimmed/${prefix}_L001_R2_001.trimmed.fastq"
	done
	)| sort) <(find "$dir_work" -type f | sort) 1>&2
}

run_demo_2
test_files
