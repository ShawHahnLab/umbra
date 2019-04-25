#!/usr/bin/env bash
set -o errexit

SEQROOT=${1-demo_1_seq}
CONFIG=${2-demo/demo_1.yml}

rsync -r test_umbra/data/ $SEQROOT
UMBRA_SYSTEM_CONFIG="" python -m umbra -c <(sed s/SEQROOT/$SEQROOT/g $CONFIG) -a process &
sleep 9
kill -s INT %1
# TODO actually test outputs
