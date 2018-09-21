import unittest
from tempfile import TemporaryDirectory
from distutils.dir_util import copy_tree, remove_tree, mkpath
from distutils.file_util import copy_file
from pathlib import Path
import re
import csv
from zipfile import ZipFile

# TODO fix this
import sys
sys.path.append(str((Path(__file__).parent/"..").resolve()))
import illumina_processor
from illumina_processor import (illumina, ProjectData, ProjectError)
from illumina_processor.illumina.run import Run
from illumina_processor.illumina.alignment import Alignment

PATH_ROOT = Path(__file__).parent / "testdata"

# Wait for enter key before removing tempdir on each test?
TMP_PAUSE = False

class TestIlluminaProcessorBase(unittest.TestCase):
    """Some setup/teardown shared with the real test classes."""

    def setUp(self):
        self.setUpTmpdir()

    def tearDown(self):
        if TMP_PAUSE:
            sys.stderr.write("\n\ntmpdir = %s\n\n" % self.tmpdir.name)
            input()
        self.tearDownTmpdir()

    def setUpTmpdir(self):
        # Make a full copy of the testdata to a temporary location
        self.tmpdir = TemporaryDirectory()
        copy_tree(PATH_ROOT, self.tmpdir.name)
        self.path = Path(self.tmpdir.name)
        self.path_runs   = Path(self.tmpdir.name) / "runs"
        self.path_exp    = Path(self.tmpdir.name) / "experiments"
        self.path_status = Path(self.tmpdir.name) / "status"
        self.path_proc   = Path(self.tmpdir.name) / "processed"
        self.path_pack   = Path(self.tmpdir.name) / "packaged"
        self.path_report = Path(self.tmpdir.name) / "report.csv"

    def tearDownTmpdir(self):
        self.tmpdir.cleanup()
