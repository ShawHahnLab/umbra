"""
Executable interface for use as a script.
"""

from .util import *
from .processor import IlluminaProcessor
from .config import (update_config, path_for_config)
from . import install
import argparse

DOCS = {}
DOCS["description"] = "Process Illumina runs."
DOCS["epilog"] = """
The actions are:

process: Continually refresh and process incoming run data.  The configuration
         defaults assume access to paths that may require the use of the
         "install" action first.
report:  Generate a single report of run information and exit.  Anyone with
         read access to the run data should be able to run this.
install: Create a systemd service entry and configure filesystem paths and
         permissions appropriately.  Ownership will be set to match that of
         the owner of the installed program.  After this you should be able to
         "systemctl start umbra" and such.  Assuming default options are in
         effect this will probably need to be executed as root.  For example:
         sudo -E $(which umbra) -a install
"""

parser = argparse.ArgumentParser(
        description=DOCS["description"],
        epilog=DOCS["epilog"],
        formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("-c", "--config", default="/etc/umbra.yml",
        help="path to configuration file")
parser.add_argument("-a", "--action", default="report",
        help="program action (default: %(default)s)",
        choices=["process", "report", "install"])
parser.add_argument("-v", "--verbose", action="count", default=0,
        help="Increment log verbosity")
parser.add_argument("-q", "--quiet", action="count", default=0,
        help="Decrement log verbosity")
parser.add_argument("-n", "--dry-run", action="store_true",
        help="Only pretend during an install action")

logger = logging.getLogger()

def setup_log(verbose, quiet):
    # Handle warnings via logging
    logging.captureWarnings(True)
    # Configure the root logger
    # each -v or -q decreases or increases the log level by 10, starting from
    # WARNING by default.
    lvl_current = logger.getEffectiveLevel()
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
        # If specific in the config, modify the log level.  Call setup_log
        # again so that the command-line flags are applied after the new level
        # is set.
        newlevel = config.get("loglevel")
        if not newlevel is None: # (since 0 is distinct from not set)
            logger.setLevel(newlevel)
            setup_log(args.verbose, args.quiet)
        action_args = config.get(args.action, {})
        if args.action == "process":
            proc = IlluminaProcessor(config["paths"]["root"], config)
            proc.watch_and_process(**action_args)
        elif args.action == "report":
            proc = IlluminaProcessor(config["paths"]["root"], config)
            proc.load()
            proc.report(**action_args)
        elif args.action == "install":
            # Set logger one increment more verbose
            lvl_current = logger.getEffectiveLevel()
            logger.setLevel(max(0, lvl_current - 10))
            if args.dry_run:
                msg = "Dry run enabled."
                msg += " Filesystem will not be changed."
                logger.info(msg)
            install.DRYRUN = args.dry_run
            install.install(config, args.config)
    except BrokenPipeError:
        pass

if __name__ == '__main__':
    main()
