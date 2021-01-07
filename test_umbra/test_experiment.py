"""
Tests for experiment metadata handlers.
"""

from collections import OrderedDict
from umbra import experiment
from .test_common import TestBaseHeavy, log_start, log_stop


class TestLoadMetadata(TestBaseHeavy):
    """Test basic metadata loading.

    Each version of metadata.csv should provide the same list of
    dictionaries.
    """

    setUpClass = classmethod(lambda cls: log_start(cls.__module__ + "." + cls.__name__))
    tearDownClass = classmethod(lambda cls: log_stop(cls.__module__ + "." + cls.__name__))

    def setUp(self):
        super().setUp()
        self.exp_path = self.paths["exp"] / "Partials_1_1_18"
        self.expected = [ \
            OrderedDict([
                ('Sample_Name', '1086S1_01'), ('Project', 'STR'),
                ('Contacts', {'Jesse': 'ancon@upenn.edu'}), ('Tasks', ['trim'])]),
            OrderedDict([
                ('Sample_Name', '1086S1_02'), ('Project', 'STR'),
                ('Contacts', {'Jesse': 'ancon@upenn.edu'}), ('Tasks', ['trim'])]),
            OrderedDict([
                ('Sample_Name', '1086S1_03'), ('Project', 'Something Else'),
                ('Contacts', {'Someone': 'person@gmail.com'}), ('Tasks', [])]),
            OrderedDict([
                ('Sample_Name', '1086S1_04'), ('Project', 'Something Else'),
                ('Contacts', {'Someone': 'person@gmail.com', 'Jesse Connell': 'ancon@upenn.edu'}),
                ('Tasks', [])])]

    def test_load_metadata(self):
        """Test basic metadata loading."""
        fp_metadata = self.exp_path / "metadata.csv"
        info = experiment.load_metadata(fp_metadata)
        self.assertEqual(self.expected, info)

    def test_load_metadata_caps(self):
        """Test metadata loading with capital letters in the Tasks column."""
        fp_metadata = self.exp_path / "metadata_caps.csv"
        info = experiment.load_metadata(fp_metadata)
        self.assertEqual(self.expected, info)

    def test_load_metadata_emptyrows(self):
        """Test metadata loading including empty rows.

        Empty rows should be silently removed.
        """
        fp_metadata = self.exp_path / "metadata_emptyrows.csv"
        info = experiment.load_metadata(fp_metadata)
        self.assertEqual(self.expected, info)

    def test_load_metadata_emptycols(self):
        """Test metadata loading including empty columns.

        Empty columns should be silently removed.
        """
        fp_metadata = self.exp_path / "metadata_emptycols.csv"
        info = experiment.load_metadata(fp_metadata)
        self.assertEqual(self.expected, info)
