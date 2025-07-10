"""
Helper functions for working with experiment metadata CSV files.

These help further parse the spreadsheet fields into lists and dictionaries to
define processing options.
"""

import csv
import re
from .illumina.util import load_csv

def _parse_contacts(text):
    """Create a dictionary of name/email pairs from contact text.

    For example:
    "Name <email@example.com>, Someone Else <user@site.gov>"
    is parsed into:
    {'Name': 'email@example.com', 'Someone Else': 'user@site.gov'}
    """

    # TODO formally fix https://github.com/ShawHahnLab/umbra/issues/124
    chunks = re.split("[,;]+", text.strip())
    contacts = {}
    for chunk in chunks:
        if not chunk:
            continue
        # There's a horrible rabbit hole to go down trying to figure out
        # parsing email addresses with regular expressions.  I don't care.
        # This is enough for us.
        match = re.match(r" *([\w ]* *[\w]+) *<(.*@.*)>", chunk)
        if match:
            # First case, Name [Lastname] <email@something>
            name = match.group(1)
            email = match.group(2)
        else:
            # Second case, just email@something
            match = re.match(r"([^@]*)(@[^@])", chunk)
            name = match.group(1)
            email = chunk
        contacts[name] = email
    return contacts

def load_metadata(path, **kwargs):
    """Load an Experiment metadata spreadsheet."""
    info = load_csv(path, csv.DictReader, **kwargs)
    # skip empty columns.  With DictReader these end up as just a stub "": ""
    # entry.
    for row in info:
        if "" in row:
            del row[""]
    # skip empty rows
    allempty = lambda r: set(r.values()) == set([""])
    info = [row for row in info if not allempty(row)]
    # parse tasks and contacts
    for row in info:
        row["Tasks"] = [task.lower() for task in row["Tasks"].split()]
        row["Contacts"] = _parse_contacts(row["Contacts"])
    return info
