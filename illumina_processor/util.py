import sys
from pathlib import Path
import re
import os
from . import illumina
from distutils.dir_util import copy_tree

def slugify(text, mask="_"):
    pat = "[^A-Za-z0-9-_]"
    safe_text = re.sub(pat, mask, text)
    return(safe_text)

def datestamp(dateobj):
    return(dateobj.strftime("%Y-%m-%d"))

def touch(path):
    mkparent(path)
    Path(path).touch()

def mkparent(path):
    parent = Path(path).parent
    os.makedirs(parent, exist_ok=True)
