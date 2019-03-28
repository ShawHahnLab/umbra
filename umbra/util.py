"""
Utility functions used throughout the package.

These are largely just wrappers for filesystem operations or text manipulation.
"""

from pathlib import Path
import re
import os
import time
import warnings
import yaml

class ProjectError(Exception):
    """Any sort of project-related exception."""

def slugify(text, mask="_"):
    """Create a short, simple text string from the given input text."""
    pat = "[^A-Za-z0-9-_]"
    safe_text = re.sub(pat, mask, text)
    return safe_text

def datestamp(dateobj):
    """Convert a date object into a text datestamp."""
    fmt = "%Y-%m-%d"
    try:
        txt = dateobj.strftime(fmt)
    except AttributeError:
        txt = time.strftime(fmt, dateobj)
    return txt

def touch(path):
    """Touch a filesystem path, making its parent if needed."""
    mkparent(path)
    Path(path).touch()

def mkparent(path):
    """Create the parent directory for a filesystem path."""
    # We need to be careful because other threads may be working in the exact
    # same directory right now.  pathlib didn't handle this correctly until
    # recently:
    # https://bugs.python.org/issue29694
    # I'll stick with the os module for now, since it seems to handle it just
    # fine.
    parent = Path(path).parent
    os.makedirs(parent, exist_ok=True)

def yaml_load(path):
    """Load YAML from a file, assuming a dictionary if empty."""
    with open(path) as fin:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            data = yaml.safe_load(fin)
    # If there's no actual yaml data in the file (like, say, just a bunch of
    # comments) we get None!  That makes it tricky later on so for our purposes
    # we'll catch that and default to a dict.
    data = data or {}
    return data
