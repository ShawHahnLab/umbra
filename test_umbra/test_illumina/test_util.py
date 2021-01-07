"""
Tests for illumina.util helper functions.
"""

import csv
from umbra.illumina import util
from . import test_common

class TestLoadCSV(test_common.TestBase):
    """Base test case for a CSV file.

    This and the child clsses test load_csv against the complexities that come
    up with loading real-life CSV files.  (There are more than I ever would
    have expected.)
    """

    def setUp(self):
        super().setUp()
        self.data_exp = [['A', 'B', 'C', 'D'], ['1', '2', '3', '4']]
        self.dict_exp = [{"A": "1", "B": "2", "C": "3", "D": "4"}]

    def test_load_csv(self):
        """Test that a list of lists is created."""
        data = util.load_csv(self.path / "test.csv")
        self.assertEqual(data, self.data_exp)

    def test_load_csv_loader(self):
        """Test that a specific reader object can be supplied."""
        data = util.load_csv(self.path / "test.csv", csv.reader)
        self.assertEqual(data, self.data_exp)

    def test_load_csv_dict_loader(self):
        """Test that a csv.DictReader works too."""
        data = util.load_csv(self.path / "test.csv", csv.DictReader)
        self.assertEqual(data, self.dict_exp)


class TestLoadCSVWindows(TestLoadCSV):
    """Test CSV loading with \\r\\n line endings.

    This should produce the same result as the original test.
    """


class TestLoadCSVMac(TestLoadCSV):
    """Test CSV loading with \\r line endings.

    This should also produce the same result as the original test.  And yes,
    this does still come up (CSV export in Microsoft Office on Mac OS).
    """


class TestLoadCSVUTF8(TestLoadCSV):
    """Test CSV loading with unicode included."""

    def setUp(self):
        # This one has greek alpha through delta as headings.
        super().setUp()
        self.data_exp = [['Α', 'Β', 'Γ', 'Δ'], ['1', '2', '3', '4']]
        self.dict_exp = [{"Α": "1", "Β": "2", "Γ": "3", "Δ": "4"}]


class TestLoadCSVUTF8BOM(TestLoadCSV):
    """Test CSV loading with a unicode byte order mark included.

    This should produce the same result as the previous UTF8 test, ignoring the
    byte order mark at the start of the file.
    """


class TestLoadCSVISO8859(TestLoadCSV):
    """Test CSV loading with non-unicode non-ASCII bytes.

    The possible options here are raise an exception, mask the unknown bytes
    with something else, or remove them.
    """

    def setUp(self):
        # Depending on the arguments this will try to handle the weird byte or
        # will raise an exception.
        super().setUp()
        self.path_csv = self.path / "test.csv"
        repl = "\N{REPLACEMENT CHARACTER}"
        self.data_exp_strip = [['A', 'B', 'C', 'D'], [''] * 4]
        self.data_exp_mask = [['A', 'B', 'C', 'D'], [repl] * 4]
        self.dict_exp_strip = [{"A": "", "B": "", "C": "", "D": ""}]
        self.dict_exp_mask = [{"A": repl, "B": repl, "C": repl, "D": repl}]

    def test_load_csv(self):
        with self.assertRaises(Exception):
            data = util.load_csv(self.path_csv)
        data = util.load_csv(self.path_csv, non_unicode="replace")
        self.assertEqual(data, self.data_exp_mask)
        data = util.load_csv(self.path_csv, non_unicode="strip")
        self.assertEqual(data, self.data_exp_strip)
        with self.assertRaises(Exception):
            data = util.load_csv(self.path_csv, non_unicode=None)

    def test_load_csv_loader(self):
        with self.assertRaises(Exception):
            data = util.load_csv(self.path_csv, csv.reader)
        data = util.load_csv(self.path_csv, csv.reader, non_unicode="replace")
        self.assertEqual(data, self.data_exp_mask)
        data = util.load_csv(self.path_csv, csv.reader, non_unicode="strip")
        self.assertEqual(data, self.data_exp_strip)
        with self.assertRaises(Exception):
            data = util.load_csv(self.path_csv, csv.reader, non_unicode=None)

    def test_load_csv_dict_loader(self):
        with self.assertRaises(Exception):
            data = util.load_csv(self.path_csv, csv.DictReader)
        data = util.load_csv(self.path_csv, csv.DictReader, non_unicode="replace")
        self.assertEqual(data, self.dict_exp_mask)
        data = util.load_csv(self.path_csv, csv.DictReader, non_unicode="strip")
        self.assertEqual(data, self.dict_exp_strip)
        with self.assertRaises(Exception):
            data = util.load_csv(self.path_csv, csv.DictReader, non_unicode=None)


class TestLoadCSVMissing(TestLoadCSV):
    """Test CSV loading for a nonexistent file.

    Any attempt should raise FileNotFoundError."""

    def test_load_csv(self):
        """Test that a list of lists is created."""
        with self.assertRaises(FileNotFoundError):
            util.load_csv(self.path / "test.csv")

    def test_load_csv_loader(self):
        """Test that a specific reader object can be supplied."""
        with self.assertRaises(FileNotFoundError):
            util.load_csv(self.path / "test.csv", csv.reader)

    def test_load_csv_dict_loader(self):
        """Test that a csv.DictReader works too."""
        with self.assertRaises(FileNotFoundError):
            util.load_csv(self.path / "test.csv", csv.DictReader)


class TestLoadCheckpoint0(test_common.TestBase):
    """Base test case for a Checkpoint.txt file.

    This and child classes test parsing of the Checkpoint.txt file that
    Illumina stores in Alignment directories.
    """

    def test_load_checkpoint(self):
        """Test that a list of lists is created."""
        data = util.load_checkpoint(self.path / "Checkpoint.txt")
        self.assertEqual(data, [0, "Demultiplexing"])


class TestLoadCheckpoint1(TestLoadCheckpoint0):
    """Try loading Checkpoint.txt for state 1."""

    def test_load_checkpoint(self):
        data = util.load_checkpoint(self.path / "Checkpoint.txt")
        self.assertEqual(data, [1, "Generating FASTQ Files"])


class TestLoadCheckpoint3(TestLoadCheckpoint0):
    """Try loading Checkpoint.txt for state 3."""

    def test_load_checkpoint(self):
        data = util.load_checkpoint(self.path / "Checkpoint.txt")
        self.assertEqual(data, [3, ""])


class TestLoadBCLStats(test_common.TestBase):
    """Base test case for a .stats file."""

    def test_load_bcl_stats(self):
        """Test that a list of dicts is created, exactly as expected."""
        expected = test_common.make_bcl_stats_dict()
        observed = util.load_bcl_stats(self.path / "base.stats")
        self.assertEqual(observed, expected)
        self.assertEqual(
            {key: type(val) for key, val in observed.items()},
            {key: type(val) for key, val in expected.items()})
