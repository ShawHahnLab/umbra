#!/usr/bin/env python
# TODO fix this
import sys
sys.path.append('/home/jesse/dev/illumina-process')
import unittest
import warnings
import datetime
import illumina
from illumina.run import Run
from pathlib import Path

RUN_IDS = {
        "MiSeq": "180101_M00000_0000_000000000-XXXXX",
        "MiniSeq": "180101_M000000_0000_XXXXXXXXXX",
        "misnamed": "run-files-custom-name",
        "not a run": "something_else",
        "nonexistent": "fictional directory"
        }

PATH_RUNS = Path(__file__).parent / "testdata/runs"

class TestRunMiSeq(unittest.TestCase):
    """Test case for a regular MiSeq Run."""

    def setUp(self):
        self.path = PATH_RUNS / RUN_IDS["MiSeq"]
        self.run = Run(self.path)

    def test_attrs(self):
        # RunInfo.xml
        # Check the Run ID.
        id_exp = RUN_IDS["MiSeq"]
        id_obs = self.run.run_info.find("Run").attrib["Id"]
        self.assertEqual(id_exp, id_obs)
        # RTAComplete.txt
        # Check the full contents.
        date = datetime.datetime(2018, 1, 1, 6, 21, 31, 705000)
        rta_exp = {"Date": date, "Version": "Illumina RTA 1.18.54"}
        rta_obs = self.run.rta_complete
        self.assertEqual(rta_exp, rta_obs)
        # CompletedJobInfo.xml
        # Check the job start/completion timestamps.
        t1_obs = self.run.completed_job_info.find("StartTime").text
        t2_obs = self.run.completed_job_info.find("CompletionTime").text
        t1_exp = "2018-01-02T06:48:22.0480092-04:00"
        t2_exp = "2018-01-02T06:48:32.608024-04:00"
        self.assertEqual(t1_exp, t1_obs)
        self.assertEqual(t2_exp, t2_obs)


class TestRunInvalid(unittest.TestCase):
    """Test case for a directory that is not an Illumina run."""

    def test_init(self):
        path = PATH_RUNS / RUN_IDS["not a run"]
        with self.assertRaises(ValueError):
            Run(path)
        path = PATH_RUNS / RUN_IDS["nonexistent"]
        with self.assertRaises(ValueError):
            Run(path)

class TestRunMisnamed(unittest.TestCase):
    """Test case for a directory whose name is not the Run ID."""

    def test_init(self):
        path = PATH_RUNS / RUN_IDS["misnamed"]
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            Run(path)
            self.assertEqual(1, len(w))


# TODO MiniSeq

if __name__ == '__main__':
    unittest.main()
