#!/usr/bin/env python3

"""
Executable interface for use as a script.
"""

import sys
from pathlib import Path
from .processor import IlluminaProcessor
from .util import *
import argparse
import yaml.parser
import collections

# Adapted from
# https://stackoverflow.com/a/3233356
def update_tree(tree_orig, tree_new):
    """Recursively update one dict with another.
    
    Note that the original is modified in place."""
    for key, val in tree_new.items():
        if isinstance(val, collections.Mapping):
            tree_orig[key] = update_tree(tree_orig.get(key, {}), val)
        else:
            tree_orig[key] = val
    return(tree_orig)

def layer_configs(paths):
    """Load configuration for each path given, merging all options.
    
    The later paths take priority."""
    config = {}
    logger = logging.getLogger()
    logger.debug('start')
    for path in paths:
        if Path(path).exists():
            try:
                c = yaml_load(path)
                logger.info("Configuration loaded from %s" % path)
            except yaml.parser.ParserError as e:
                logger.critical("Configuration parse error while loading %s" % path)
                raise(e)
        else:
            logger.info("Configuration file not found at %s" % path)
            c = {}
        x = update_tree(config, c)
        #config.update(c)
    return(config)

def update_config(config_path, args):
    """Load a config file layered with package defaults."""
    # First load package defaults including action-specific if available
    root = Path(__file__).parent / "data"
    path_config = root / "config.yml"
    path_action = root / ("config_" + args.action + ".yml")
    paths = [path_config, path_action, config_path]
    # Add in the supplied config
    config = layer_configs(paths)
    #c.update(config)
    return(config)

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
