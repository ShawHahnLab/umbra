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
