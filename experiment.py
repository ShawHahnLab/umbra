from util import *
import csv

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
        # There's a horrible rabbit hole to go down trying to figure out
        # parsing email addresses with regular expressions.  I don't care.
        # This is enough for us.
        m = re.match(" *([\w ]* *[\w]+) *<(.*@.*)>", chunk)
        name = m.group(1)
        email = m.group(2)
        contacts[name] = email
    return(contacts)

def load_metadata(path):
    """Load an Experiment metadata spreadsheet."""
    info = illumina.load_csv(path, csv.DictReader)
    for row in info:
        row["Tasks"] = row["Tasks"].split()
        row["Contacts"] = _parse_contacts(row["Contacts"])
    return(info)
