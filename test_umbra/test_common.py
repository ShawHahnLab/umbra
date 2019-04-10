"""
Common test code shared with the real tests.  Not much to see here.
"""

import unittest
from tempfile import TemporaryDirectory
from distutils.dir_util import copy_tree
from pathlib import Path
import hashlib
import sys
from umbra import util

PATH_ROOT = Path(__file__).parent
PATH_DATA = PATH_ROOT / "data"
PATH_CONFIG = PATH_ROOT / ".." / "test_config.yml"

# Wait for enter key before removing tempdir on each test?
TMP_PAUSE = False

CONFIG = util.yaml_load(PATH_CONFIG)

def md5(text):
    """MD5 Checksum of the given text."""
    try:
        text = text.encode("utf-8")
    except AttributeError:
        pass
    return hashlib.md5(text).hexdigest()

class TestBase(unittest.TestCase):
    """Some setup/teardown shared with the real test classes."""

    def setUp(self):
        self.set_up_tmpdir()
        self.set_up_vars()

    def tearDown(self):
        if TMP_PAUSE:
            sys.stderr.write("\n\ntmpdir = %s\n\n" % self.tmpdir.name)
            input()
        self.tear_down_tmpdir()

    def set_up_tmpdir(self):
        """Make a full copy of the testdata to a temporary location."""
        self.tmpdir = TemporaryDirectory()
        copy_tree(PATH_DATA, self.tmpdir.name)
        self.paths = {
            "top":  Path(self.tmpdir.name),
            "runs": Path(self.tmpdir.name) / "runs",
            "exp": Path(self.tmpdir.name) / "experiments",
            "status": Path(self.tmpdir.name) / "status",
            "proc": Path(self.tmpdir.name) / "processed",
            "pack": Path(self.tmpdir.name) / "packaged",
            "report": Path(self.tmpdir.name) / "report.csv"
            }

    def set_up_vars(self):
        """Initial variables for testing comparisons."""
        self.mails = []

    def tear_down_tmpdir(self):
        """Clean up temporary directory on disk."""
        # There's a bug where it'll raise a PermissionError if anything
        # write-protected ended up in the tmpdir.  So don't leave anything like
        # that lying around during a test!
        # https://bugs.python.org/issue26660
        self.tmpdir.cleanup()

    @staticmethod
    def uploader(path):
        """Mock of Box uploader function.

        This generates a Box-like URL for a supposedly-successful upload."""
        prefix = "https://domain.box.com/shared/static/"
        # Box uses 32 lowercase alphanumeric characters (a-z, 0-9).  Not sure
        # what its method is but I'll just do an md5sum here.
        checksum = md5(Path(path).name)
        suffix = Path(path).suffix
        url = prefix + checksum + suffix
        return url

    def mailer(self, **kwargs):
        """Mock of Mailer.mail function.

        This accepts email parameters and just stores them."""
        self.mails.append(kwargs)
        return kwargs
