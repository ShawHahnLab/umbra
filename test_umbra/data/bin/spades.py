#!/usr/bin/env python
"""
Mock spades.py to test TaskSpades class.
"""

import os
import sys

#["--12",
#"$TEST_TMPDIR/proc/PairedReads/sample_S1_L001_R_001.merged.fastq",
#"-o",
#"$TEST_TMPDIR/proc/assembled/sample_S1_L001_R_001",
#"-t",
#"1",
#"--phred-offset",
#"33"]

def parse_args(args):
    args_out = []
    for item in args:
        if item.startswith(os.environ["TEST_TMPDIR"]):
            item = "$TEST_TMPDIR" + item[len(os.environ["TEST_TMPDIR"]):]
        args_out.append(item)
    return args_out

def main():
    with open(os.environ["TEST_LOG"], "wt") as log:
        args = parse_args(sys.argv[1:])
        for item in args:
            log.write(item + "\n")

main()
