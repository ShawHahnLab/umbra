"""
Configuration file handling.
"""
from .util import *
import collections
import yaml.parser

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
        update_tree(config, c)
    return(config)

def path_for_config(suffix=None):
    if suffix:
        name = "config_%s.yml" % suffix
    else:
        name = "config.yml"
    path = Path(__file__).parent / "data" / name
    return(path)

def update_config(config_path, args):
    """Load a config file layered with package defaults."""
    # In order, load package defaults (including action-specific if available)
    # and then layer in the supplied config.
    paths = [path_for_config(), path_for_config(args.action), config_path]
    config = layer_configs(paths)
    return(config)
