import xml.etree.ElementTree
import csv
import re
import datetime
import time
from pathlib import Path

ADAPTERS = {
        "Nextera": [
            "CTGTCTCTTATACACATCTCCGAGCCCACGAGAC",
            "CTGTCTCTTATACACATCTGACGCTGCCGACGA"
            ]
        }

def load_xml(path):
    e = xml.etree.ElementTree.parse(path).getroot()
    return(e)

def load_csv(path, loader=csv.reader):
    """Load CSV data from a given file path.
    
    By default returns a list of lists using csv.reader, but another reader
    than can operate on a file object (e.g. csv.DictReader) can be supplied
    instead.  Supports UTF8 (with or without a byte order mark) and
    equivalently ASCII.
    """
    # Explicitly setting the encoding to utf-8-sig allows the byte order mark
    # to be automatically stripped out if present.
    with open(path, 'r', newline='', encoding='utf-8-sig') as f:
        data = [row for row in (loader(f))]
    return(data)

def load_sample_sheet(path):
    """Load an Illumina CSV Sample Sheet.
    
    The data is returned as a dictionary, using the named sections in the sheet
    as the keys. Recognized sections are parsed further, and anything else is
    left as lists-of-lists.
    
    Header: dictionary with string values
    Reads: list of integer lengths
    Settings: dictionary with string values (not in MiniSeq)
    Data: list of dictionaries using the first row as keys
    """
    data_raw = load_csv(path)
    data = {}
    for row in data_raw:
        if not len(row):
            continue
        # Check for section name like [Header].  If found, initialize a section
        # with that name.
        m = re.match("\\[([A-Za-z0-9]+)\\]", row[0])
        if m:
            name = m.group(1)
            data[name] = []
        # Otherwise, append non-empty rows to the current named section.
        else:
            if sum([len(x) for x in row]) > 0:
                data[name] += [row]

    # Convert Header and Settings to dictionaries and Reads to a simple list
    def parse_dict_fields(rows):
        fields = {}
        for row in rows:
            if len(row) == 0:
                continue
            elif len(row) == 1:
                fields[row[0]] = ""
            else:
                fields[row[0]] = row[1]
        return(fields)

    data["Header"] = parse_dict_fields(data["Header"])
    data["Reads"]  = [int(row[0]) for row in data["Reads"]]
    if "Settings" in data.keys():
        data["Settings"] = parse_dict_fields(data["Settings"])

    # Convert the Data section into a list of dictionaries
    cols = data["Data"].pop(0)
    samples = []
    for row in data["Data"]:
        samples.append({k: v for k, v in zip(cols, row)})
    data["Data"] = samples

    return(data)
