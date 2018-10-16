import sys
from pathlib import Path
import re
import os
import time
import warnings
import yaml
from . import illumina
from distutils.dir_util import copy_tree

def slugify(text, mask="_"):
    pat = "[^A-Za-z0-9-_]"
    safe_text = re.sub(pat, mask, text)
    return(safe_text)

def datestamp(dateobj):
    fmt = "%Y-%m-%d"
    try:
        txt = dateobj.strftime(fmt)
    except AttributeError:
        txt = time.strftime(fmt, dateobj)
    return txt

def touch(path):
    mkparent(path)
    Path(path).touch()

def mkparent(path):
    parent = Path(path).parent
    os.makedirs(parent, exist_ok=True)

def yaml_load(path):
    with open(path) as f:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore",category=DeprecationWarning)
            data = yaml.safe_load(f)
    return(data)
