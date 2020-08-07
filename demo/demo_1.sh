#!/usr/bin/env bash

# A very basic test, using the test dataset and a simple config.

source demo/demo_setup.sh

SEQROOT=${1:-demo_1_seq}
CONFIG=${2:-demo/demo_1.yml}
DELAY=${3:-1}

function run_demo_1 {
	rsync -r test_umbra/data/ $SEQROOT
	run_umbra "$SEQROOT" "$CONFIG" "$DELAY"
}

# TODO actually test outputs
run_demo_1
