"""
Executable interface for use as a script.
"""

from .util import *
from .processor import IlluminaProcessor
from . import config
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
parser.add_argument("-c", "--config", help="path to configuration file")
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
        # In order, layer together the package default, system default, action
        # default, and command-line config path (if present).
        cpaths = [
                config.path_for_config(),
                config.SYSTEM_CONFIG,
                config.path_for_config(args.action),
                args.config]
        conf = config.layer_configs(cpaths)
        # If specific in the config, modify the log level.  Call setup_log
        # again so that the command-line flags are applied after the new level
        # is set.
        newlevel = conf.get("loglevel")
        if not newlevel is None: # (since 0 is distinct from not set)
            logger.setLevel(newlevel)
            setup_log(args.verbose, args.quiet)
        action_args = conf.get(args.action, {})
        if args.action == "process":
            proc = IlluminaProcessor(conf["paths"]["root"], conf)
            proc.watch_and_process(**action_args)
        elif args.action == "report":
            proc = IlluminaProcessor(conf["paths"]["root"], conf)
            proc.load()
            proc.report(**action_args)
        elif args.action == "install":
            if args.config:
                msg = "Custom configuration not applicable during install."
                msg += " To use a configuration during and after install,"
                msg += " place settings in %s, or to use a configuration"
                msg += " after installation, modify the path after installation."
                msg = msg % config.SYSTEM_CONFIG
                logger.critical(msg)
                sys.exit(1)
            # Set logger one increment more verbose
            lvl_current = logger.getEffectiveLevel()
            logger.setLevel(max(0, lvl_current - 10))
            if args.dry_run:
                msg = "Dry run enabled."
                msg += " Filesystem will not be changed."
                logger.info(msg)
            install.DRYRUN = args.dry_run
            install.install(conf, args.config)
    except BrokenPipeError:
        pass

if __name__ == '__main__':
    main()
