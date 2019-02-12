"""
Helper functions for working with experiment metadata CSV files.

These help further parse the spreadsheet fields into lists and dictionaries to
define processing options.
"""

import csv
import re
from . import illumina

def _parse_contacts(text):
    """Create a dictionary of name/email pairs from contact text.

    For example:
    "Name <email@example.com>, Someone Else <user@site.gov>"
    is parsed into:
    {'Name': 'email@example.com', 'Someone Else': 'user@site.gov'}
    """

    chunks = re.split("[,;]+", text)
    contacts = {}
    for chunk in chunks:
        if not chunk:
            continue
        # There's a horrible rabbit hole to go down trying to figure out
        # parsing email addresses with regular expressions.  I don't care.
        # This is enough for us.
        match = re.match(r" *([\w ]* *[\w]+) *<(.*@.*)>", chunk)
        name = match.group(1)
        email = match.group(2)
        contacts[name] = email
    return contacts

def load_metadata(path):
    """Load an Experiment metadata spreadsheet."""
    info = illumina.util.load_csv(path, csv.DictReader)
    for row in info:
        row["Tasks"] = row["Tasks"].split()
        row["Contacts"] = _parse_contacts(row["Contacts"])
    return info
