"""
Tests for illumina.util helper functions.
"""

import unittest
import csv
from umbra.illumina import util
from .test_common import PATH_OTHER

class TestLoadCSV(unittest.TestCase):
    """Base test case for a CSV file.

    This and the child clsses test load_csv against the complexities that come
    up with loading real-life CSV files.  (There are more than I ever would
    have expected.)
    """

    def setUp(self):
        self.path = PATH_OTHER / "test.csv"
        self.data_exp = [['A', 'B', 'C', 'D'], ['1', '2', '3', '4']]
        self.dict_exp = [{"A": "1", "B": "2", "C": "3", "D": "4"}]

    def test_load_csv(self):
        """Test that a list of lists is created."""
        data = util.load_csv(self.path)
        self.assertEqual(data, self.data_exp)

    def test_load_csv_loader(self):
        """Test that a specific reader object can be supplied."""
        data = util.load_csv(self.path, csv.reader)
        self.assertEqual(data, self.data_exp)

    def test_load_csv_dict_loader(self):
        """Test that a csv.DictReader works too."""
        data = util.load_csv(self.path, csv.DictReader)
        self.assertEqual(data, self.dict_exp)


class TestLoadCSVWindows(TestLoadCSV):
    """Test CSV loading with \\r\\n line endings."""

    def setUp(self):
        # This should produce the same result as the original test.
        super().setUp()
        self.path = PATH_OTHER / "test_rn.csv"


class TestLoadCSVMac(TestLoadCSV):
    """Test CSV loading with \\r line endings.

    Yes this does still come up (CSV export in Microsoft Office on Mac OS)."""

    def setUp(self):
        # This should produce the same result as the original test.
        super().setUp()
        self.path = PATH_OTHER / "test_r.csv"


class TestLoadCSVUTF8(TestLoadCSV):
    """Test CSV loading with unicode included."""

    def setUp(self):
        # This one has greek alpha through delta as headings.
        self.path = PATH_OTHER / "test_utf8.csv"
        self.data_exp = [['Α', 'Β', 'Γ', 'Δ'], ['1', '2', '3', '4']]
        self.dict_exp = [{"Α": "1", "Β": "2", "Γ": "3", "Δ": "4"}]


class TestLoadCSVUTF8BOM(TestLoadCSV):
    """Test CSV loading with a unicode byte order mark included."""

    def setUp(self):
        # This should produce the same result as the original test, ignoring
        # the byte order mark at the start of the file.
        # See #26.
        super().setUp()
        self.path = PATH_OTHER / "test_utf8bom.csv"


class TestLoadCSVMissing(TestLoadCSV):
    """Test CSV loading for a nonexistent file.

    Any attempt should raise FileNotFoundError."""

    def setUp(self):
        # This should produce the same result as the original test.
        super().setUp()
        self.path = PATH_OTHER / "test_missing.csv"

    def test_load_csv(self):
        """Test that a list of lists is created."""
        with self.assertRaises(FileNotFoundError):
            util.load_csv(self.path)

    def test_load_csv_loader(self):
        """Test that a specific reader object can be supplied."""
        with self.assertRaises(FileNotFoundError):
            util.load_csv(self.path, csv.reader)

    def test_load_csv_dict_loader(self):
        """Test that a csv.DictReader works too."""
        with self.assertRaises(FileNotFoundError):
            util.load_csv(self.path, csv.DictReader)


class TestLoadCheckpoint0(unittest.TestCase):
    """Base test case for a Checkpoint.txt file.

    This and child classes test parsing of the Checkpoint.txt file that
    Illumina stores in Alignment directories.
    """

    def setUp(self):
        self.path = PATH_OTHER / "checkpoints" / "Checkpoint0.txt"
        self.data_exp = [0, "Demultiplexing"]

    def test_load_checkpoint(self):
        """Test that a list of lists is created."""
        data = util.load_checkpoint(self.path)
        self.assertEqual(data, self.data_exp)


class TestLoadCheckpoint1(TestLoadCheckpoint0):
    """Try loading Checkpoint.txt for state 1."""

    def setUp(self):
        self.path = PATH_OTHER / "checkpoints" / "Checkpoint1.txt"
        self.data_exp = [1, "Generating FASTQ Files"]


class TestLoadCheckpoint3(TestLoadCheckpoint0):
    """Try loading Checkpoint.txt for state 3."""

    def setUp(self):
        self.path = PATH_OTHER / "checkpoints" / "Checkpoint3.txt"
        self.data_exp = [3, ""]
