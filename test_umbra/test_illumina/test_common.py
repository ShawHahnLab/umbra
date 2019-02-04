import warnings
import datetime
from tempfile import TemporaryDirectory
from shutil import move
import os
from ..test_common import *

from umbra.illumina.run import Run, Alignment

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
