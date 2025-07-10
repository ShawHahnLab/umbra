"""
Tests for experiment metadata handlers.
"""

from collections import OrderedDict
from umbra import experiment
from .test_common import TestBase

class TestLoadMetadata(TestBase):
    """Test basic metadata loading."""

    def test_load_metadata(self):
        """Test basic metadata loading."""
        fp_metadata = self.path / "metadata.csv"
        info = experiment.load_metadata(fp_metadata)
        expected = [ \
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
        self.assertEqual(expected, info)


class TestLoadMetadataCaps(TestLoadMetadata):
    """Test metadata loading with capital letters in the Tasks column."""


class TestLoadMetadataEmptyrows(TestLoadMetadata):
    """Test metadata loading including empty rows.

    Empty rows should be silently removed.
    """


class TestLoadMetadataEmptycols(TestLoadMetadata):
    """Test metadata loading including empty columns.

    Empty columns should be silently removed.
    """

class TestLoadMetadataContactsWhitespace(TestLoadMetadata):
    """Test metadata loading including leading/trailing whitespace in Contacts.

    The whitespace should be stripped out, and all-whitespace entries should be
    equivalent to rows with no contact info supplied.
    """

    def test_load_metadata(self):
        fp_metadata = self.path / "metadata.csv"
        info = experiment.load_metadata(fp_metadata)
        expected = [ \
            OrderedDict([
                ('Sample_Name', '1086S1_01'), ('Project', 'STR'),
                ('Contacts', {'Jesse': 'ancon@upenn.edu'}), ('Tasks', ['trim'])]),
            OrderedDict([
                ('Sample_Name', '1086S1_02'), ('Project', 'STR'),
                ('Contacts', {'Jesse': 'ancon@upenn.edu'}), ('Tasks', ['trim'])]),
            OrderedDict([
                ('Sample_Name', '1086S1_03'), ('Project', 'Something Else'),
                ('Contacts', {}), ('Tasks', [])]),
            OrderedDict([
                ('Sample_Name', '1086S1_04'), ('Project', 'Something Else'),
                ('Contacts', {}),
                ('Tasks', [])])]
        self.assertEqual(expected, info)


class TestLoadMetadataISO8859(TestLoadMetadata):
    """Test metadata loading with non-unicode file.

    By default the file parsing should fail and raise an exception, but we can
    override the behavior.
    """

    def test_load_metadata(self):
        """Test basic metadata loading."""
        fp_metadata = self.path / "metadata.csv"
        with self.assertRaises(UnicodeDecodeError):
            info = experiment.load_metadata(fp_metadata)
        info = experiment.load_metadata(fp_metadata, non_unicode="strip")
        expected = [ \
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
        self.assertEqual(expected, info)
