"""
Helpers for other test modules.
"""

import unittest
from pathlib import Path
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

def make_bcl_stats_dict(**kwargs):
    """Set up a default BCL stats dictionary with zeros."""
    data = {
        'cycle': 0,
        'avg_intensity': 0.0,
        'avg_int_all_A': 0.0,
        'avg_int_all_C': 0.0,
        'avg_int_all_G': 0.0,
        'avg_int_all_T': 0.0,
        'avg_int_cluster_A': 0.0,
        'avg_int_cluster_C': 0.0,
        'avg_int_cluster_G': 0.0,
        'avg_int_cluster_T': 0.0,
        'num_clust_call_A': 0,
        'num_clust_call_C': 0,
        'num_clust_call_G': 0,
        'num_clust_call_T': 0,
        'num_clust_call_X': 0,
        'num_clust_int_A': 0,
        'num_clust_int_C': 0,
        'num_clust_int_G': 0,
        'num_clust_int_T': 0}
    data.update(kwargs)
    return data


def dummy_bcl_stats(cycles, lanes):
    """Build mock bcl stats list with zeros."""
    expected = []
    for lane in range(1, lanes+1):
        for cycle in range(cycles):
            for tile in [1101, 1102]:
                bcl = make_bcl_stats_dict(cycle=cycle, lane=lane, tile=tile)
                expected.append(bcl)
    return expected

class TestBase(unittest.TestCase):
    """Helper for tracking test case duration."""

    setUpClass = classmethod(lambda cls: log_start(cls.__module__ + "." + cls.__name__))
    tearDownClass = classmethod(lambda cls: log_stop(cls.__module__ + "." + cls.__name__))

    @property
    def path(self):
        """Path for supporting files for each class."""
        path = self.__class__.__module__.split(".") + [self.__class__.__name__]
        path.insert(1, "data")
        path = Path("/".join(path))
        return path
