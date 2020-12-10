"""
Helpers for other test modules.
"""

import unittest
from ..test_common import PATH_DATA, log_start, log_stop

RUN_IDS = {
        "MiSeq":       "180101_M00000_0000_000000000-XXXXX",
        "MiniSeq":     "180103_M000000_0000_0000000000",
        "Single":      "180105_M00000_0000_000000000-XXXXX",
        "misnamed":    "run-files-custom-name",
        "not a run":   "something_else",
        "nonexistent": "fictional directory"
        }

PATH_RUNS = PATH_DATA / "runs"
PATH_OTHER = PATH_DATA / "other"


class TestBase(unittest.TestCase):
    """Helper for tracking test case duration."""

    setUpClass = classmethod(lambda cls: log_start(cls.__module__ + "." + cls.__name__))
    tearDownClass = classmethod(lambda cls: log_stop(cls.__module__ + "." + cls.__name__))
