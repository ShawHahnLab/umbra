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
    with open(path, 'r', newline='') as f:
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
        m = re.match("\\[([A-Za-z0-9]+)\\]", row[0])
        if m:
            name = m.group(1)
            data[name] = []
        else:
            # skip empty rows
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
