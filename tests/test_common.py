"""
Common test code shared with the real tests.  Not much to see here.
"""

import unittest
from tempfile import TemporaryDirectory, NamedTemporaryFile
from distutils.dir_util import copy_tree, remove_tree, mkpath
from distutils.file_util import copy_file
from pathlib import Path
import re
import csv
from zipfile import ZipFile
import hashlib

# TODO fix this
import sys
sys.path.append(str((Path(__file__).parent/"..").resolve()))
import umbra
from umbra import (illumina, ProjectData, ProjectError, util)
from umbra.illumina.run import Run
from umbra.illumina.alignment import Alignment

PATH_ROOT = Path(__file__).parent / ".."
PATH_DATA = PATH_ROOT / "testdata"
PATH_CONFIG = PATH_ROOT / "test_config.yml"

# Wait for enter key before removing tempdir on each test?
TMP_PAUSE = False

CONFIG = util.yaml_load(PATH_CONFIG)

def md5(text):
    """MD5 Checksum of the given text."""
    return(hashlib.md5(text.encode("utf-8")).hexdigest())

class TestBase(unittest.TestCase):
    """Some setup/teardown shared with the real test classes."""

    def setUp(self):
        self.mails = []
        self.setUpTmpdir()

    def tearDown(self):
        if TMP_PAUSE:
            sys.stderr.write("\n\ntmpdir = %s\n\n" % self.tmpdir.name)
            input()
        self.tearDownTmpdir()

    def setUpTmpdir(self):
        # Make a full copy of the testdata to a temporary location
        self.tmpdir = TemporaryDirectory()
        copy_tree(PATH_DATA, self.tmpdir.name)
        self.path = Path(self.tmpdir.name)
        self.path_runs   = Path(self.tmpdir.name) / "runs"
        self.path_exp    = Path(self.tmpdir.name) / "experiments"
        self.path_status = Path(self.tmpdir.name) / "status"
        self.path_proc   = Path(self.tmpdir.name) / "processed"
        self.path_pack   = Path(self.tmpdir.name) / "packaged"
        self.path_report = Path(self.tmpdir.name) / "report.csv"

    def tearDownTmpdir(self):
        self.tmpdir.cleanup()

    def uploader(self, path):
        """Mock of Box uploader function.
        
        This generates a Box-like URL for a supposedly-successful upload."""
        prefix = "https://domain.box.com/shared/static/"
        # Box uses 32 lowercase alphanumeric characters (a-z, 0-9).  Not sure
        # what its method is but I'll just do an md5sum here.
        checksum = "c9qce8ormkrma3yiy4t009ej9socz2xo"
        #checksum = hashlib.md5(Path(path).name.encode("utf-8")).hexdigest()
        checksum = md5(Path(path).name)
        suffix = Path(path).suffix
        url = prefix + checksum + suffix
        return(url)

    def mailer(self, **kwargs):
        """Mock of Mailer.mail function.
        
        This accepts email parameters and just stores them."""
        if not hasattr(self, "mails"):
            self.mails = []
        self.mails.append(kwargs)
        return(kwargs)
