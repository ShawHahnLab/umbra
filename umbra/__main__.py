#!/usr/bin/env python3

"""
Executable interface for use as a script.
"""

from .util import *
from .processor import IlluminaProcessor
from .config import update_config
import argparse

parser = argparse.ArgumentParser(description='Process Illumina runs.')
parser.add_argument("-c", "--config", default="/etc/umbra.yml", help="path to configuration file")
parser.add_argument("-a", "--action", default="report", help="program action", choices=["process", "report", "daemon"])
parser.add_argument("-v", "--verbose", action="count", default=0, help="Increment log verbosity")
parser.add_argument("-q", "--quiet", action="count", default=0, help="Decrement log verbosity")

def setup_log(verbose, quiet):
    # Handle warnings via logging
    logging.captureWarnings(True)
    # Configure the root logger
    # each -v or -q decreases or increases the log level by 10, starting from
    # WARNING by default.
    lvl_current = logging.getLogger().getEffectiveLevel()
    lvl_subtract = (verbose - quiet) * 10
    verbosity = max(0, lvl_current - lvl_subtract)
    logging.basicConfig(stream=sys.stderr, level = verbosity)

def main(args_raw=None):
    try:
        if args_raw:
            args = parser.parse_args(args_raw)
        else:
            args = parser.parse_args()
        setup_log(args.verbose, args.quiet)
        config = update_config(args.config, args)
        proc = IlluminaProcessor(config["paths"]["root"], config)
        if args.action in ["process", "daemon"]:
            args = config["process"]
            proc.watch_and_process(**args)
        elif args.action == "report":
            proc.load()
            args = config["report"]
            proc.report(**args)
    except BrokenPipeError:
        pass

if __name__ == '__main__':
    main()
