#!/usr/bin/env python3

"""
Executable interface for use as a script.
"""

import sys
from pathlib import Path
from . import IlluminaProcessor
from .util import *
import argparse

class ConfigAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        config = config_load(values)
        setattr(namespace, self.dest, config)


parser = argparse.ArgumentParser(description='Process Illumina runs.')
parser.add_argument('config', action=ConfigAction, help='path to configuration file')
parser.add_argument("-v", "--verbose", action="count", default=0, help="Increment log verbosity")
parser.add_argument("-q", "--quiet", action="count", default=0, help="Decrement log verbosity")

def setup_log(verbose, quiet):
    # each -v or -q decreases or increases the log level by 10, starting from
    # WARNING by default.
    lvl_current = logging.getLogger().getEffectiveLevel()
    lvl_subtract = (verbose - quiet) * 10
    verbosity = max(0, lvl_current - lvl_subtract)
    logging.basicConfig(stream=sys.stdout, level = verbosity)

def main(args_raw):
    args = parser.parse_args(args_raw)
    setup_log(args.verbose, args.quiet)
    proc = IlluminaProcessor(args.config["paths"]["root"], args.config)
    proc.watch_and_process()

if __name__ == '__main__':
    main(sys.argv[1:])
