#!/usr/bin/env python3

"""
Executable interface for use as a script.
"""

import sys
from pathlib import Path
from . import IlluminaProcessor
from .util import *
import argparse

CONFIG_DEFAULTS = {}
CONFIG_DEFAULTS["report"] = {
        "readonly": True
        }
CONFIG_DEFAULTS["daemon"] = {
        "save_report": {
          "path": "/var/log/umbra/report.csv",
          "max_width": 0
          },
        "box": {
          "credentials_path": "/var/lib/umbra/.boxcreds.yml"
          }
        }

class ConfigAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        config = config_load(values)
        setattr(namespace, self.dest, config)


def update_config(config, args):
    """Modify a config dict using command-line arguments."""
    c = CONFIG_DEFAULTS.get(args.action, {})
    c = c.copy()
    c.update(config)
    return(config)

parser = argparse.ArgumentParser(description='Process Illumina runs.')
parser.add_argument("-c", "--config", action=ConfigAction, default="/etc/umbra.yml", help="path to configuration file")
parser.add_argument("-a", "--action", default="process", help="program action", choices=["process", "report", "daemon"])
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
        # argparse doesn't call the action if the argument was left at its default.
        # https://stackoverflow.com/a/21588198
        # Maybe just ditch the custom action anyway.
        if type(args.config) is str:
            args.config = config_load(args.config)
        setup_log(args.verbose, args.quiet)
        config = update_config(args.config, args)
        proc = IlluminaProcessor(config["paths"]["root"], config)
        if args.action in ["process", "daemon"]:
            proc.watch_and_process()
        elif args.action == "report":
            proc.load()
            proc.report()
    except BrokenPipeError:
        pass

if __name__ == '__main__':
    main()
