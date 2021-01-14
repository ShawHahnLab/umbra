"""
Common test code shared with the real tests.  Not much to see here.
"""

import datetime
import time
import unittest
import logging
from tempfile import TemporaryDirectory
from distutils.dir_util import copy_tree
from pathlib import Path
import hashlib
import sys
from umbra import util

PATH_ROOT = Path(__file__).parent
PATH_DATA = PATH_ROOT / "data"
PATH_CONFIG = PATH_ROOT / ".." / "test_config.yml"

CONFIG = util.yaml_load(PATH_CONFIG)

TESTLOGGER = logging.getLogger(__name__)
TESTLOGGER.propagate = False
TESTLOGGER.setLevel(logging.DEBUG)
if CONFIG["logfile"]:
    TESTLOGGER.addHandler(
        logging.StreamHandler(open(CONFIG["logfile"], "at", buffering=1)))

def md5(text):
    """MD5 Checksum of the given text."""
    try:
        text = text.encode("utf-8")
    except AttributeError:
        pass
    return hashlib.md5(text).hexdigest()


class DumbLogHandler(logging.Handler):
    """A log handler that just stacks log records into a list."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def has_message_text(self, txt):
        """Does some text appear in any of the records?"""
        return True in [txt in rec.msg for rec in self.records]

TIMINGS = {}

def log_start(name):
    """Store a global time reference under a given name."""
    TIMINGS[name] = time.perf_counter()

def log_stop(name):
    """Log elapsed time since log_start(name) and del name reference."""
    then = TIMINGS.get(name)
    if then:
        now = datetime.datetime.now()
        delta = time.perf_counter() - then
        TESTLOGGER.info("%s, %12.8f seconds, %s", now, delta, name)
        del TIMINGS[name]


class TestBase(unittest.TestCase):
    """Helper for test cases

    This tracks test case duration by logging the time between class setup and
    teardown and provides a default file path for supporting files for each
    test, based on the class name.
    """

    setUpClass = classmethod(lambda cls: log_start(cls.__module__ + "." + cls.__name__))
    tearDownClass = classmethod(lambda cls: log_stop(cls.__module__ + "." + cls.__name__))

    @property
    def path(self):
        """Path for supporting files for each class."""
        path = self.__class__.__module__.split(".") + [self.__class__.__name__]
        path.insert(1, "data")
        path = Path("/".join(path))
        return path


class TestBaseHeavy(TestBase):
    """Legacy base class for my awfully convoluted original tests."""

    def setUp(self):
        self.set_up_tmpdir()
        self.set_up_vars()

    def tearDown(self):
        if CONFIG.get("pause"):
            sys.stderr.write("\n\ntmpdir = %s\n\n" % self.tmpdir.name)
            input()
        self.tear_down_tmpdir()

    def set_up_tmpdir(self):
        """Make a full copy of the testdata to a temporary location."""
        self.tmpdir = TemporaryDirectory()
        copy_tree(str(PATH_DATA / "experiments"), self.tmpdir.name + "/experiments")
        copy_tree(str(PATH_DATA / "status"), self.tmpdir.name + "/status")
        copy_tree(str(PATH_DATA / "processed"), self.tmpdir.name + "/processed")
        copy_tree(str(PATH_DATA / "packaged"), self.tmpdir.name + "/packaged")
        # Account for the extra layer of nested directories
        for category in ["miseq", "miniseq"]:
            thing = PATH_DATA / "runs" / category
            for run in thing.glob("*"):
                #newpath = (run / ".." / ".." / run.name).resolve()
                copy_tree(str(run), self.tmpdir.name + "/runs/" + run.name)
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
