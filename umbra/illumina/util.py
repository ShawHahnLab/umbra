"""
Utility functions used throughout the package.

These are largely just wrappers for filesystem operations or text manipulation.
"""

from pathlib import Path
import xml.etree.ElementTree
import datetime
import csv
import re

ADAPTERS = {
        "Nextera": [
            "CTGTCTCTTATACACATCTCCGAGCCCACGAGAC",
            "CTGTCTCTTATACACATCTGACGCTGCCGACGA"
            ]
        }

def load_xml(path):
    """Load an XML file and return the root element."""
    elem = xml.etree.ElementTree.parse(path).getroot()
    return elem

def load_csv(path, loader=csv.reader):
    """Load CSV data from a given file path.

    By default returns a list of lists using csv.reader, but another reader
    than can operate on a file object (e.g. csv.DictReader) can be supplied
    instead.  Supports UTF8 (with or without a byte order mark) and
    equivalently ASCII.
    """
    # Explicitly setting the encoding to utf-8-sig allows the byte order mark
    # to be automatically stripped out if present.
    with open(path, 'r', newline='', encoding='utf-8-sig') as fin:
        data = [row for row in loader(fin)]
    return data

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
        if not row:
            continue
        # Check for section name like [Header].  If found, initialize a section
        # with that name.
        match = re.match("\\[([A-Za-z0-9]+)\\]", row[0])
        if match:
            name = match.group(1)
            data[name] = []
        # Otherwise, append non-empty rows to the current named section.
        else:
            if sum([len(x) for x in row]) > 0:
                data[name] += [row]

    # Convert Header and Settings to dictionaries and Reads to a simple list
    def parse_dict_fields(rows):
        fields = {}
        for row in rows:
            if not row:
                continue
            elif len(row) == 1:
                fields[row[0]] = ""
            else:
                fields[row[0]] = row[1]
        return fields

    data["Header"] = parse_dict_fields(data["Header"])
    data["Reads"] = [int(row[0]) for row in data["Reads"]]
    if "Settings" in data.keys():
        data["Settings"] = parse_dict_fields(data["Settings"])

    # Convert the Data section into a list of dictionaries
    cols = data["Data"].pop(0)
    samples = []
    for row in data["Data"]:
        samples.append({k: v for k, v in zip(cols, row)})
    data["Data"] = samples

    return data

def load_rta_complete(path):
    """Parse an RTAComplete.txt file.

    Creates a dictionary with the Date and Illumina Real-Time Analysis
    software version.  This file should exist for a run if real-time analysis
    that does basecalling and generates BCL files has finished.
    """
    try:
        data = load_csv(path)[0]
    except (FileNotFoundError, IndexError):
        return None
    date_pad = lambda txt: "/".join([x.zfill(2) for x in txt.split("/")])
    time_pad = lambda txt: ":".join([x.zfill(2) for x in txt.split(":")])
    # MiniSeq (RTA 2x?)
    # RTA 2.8.6 completed on 3/17/2017 8:19:33 AM
    if len(data) == 1:
        match = re.match("(RTA [0-9.]+) completed on ([^ ]+) (.+)", data[0])
        version = match.group(1)
        date_str_date = date_pad(match.group(2))
        date_str_time = time_pad(match.group(3))
        date_str = date_str_date + " " + date_str_time
        fmt = '%m/%d/%Y %I:%M:%S %p'
        date_obj = datetime.datetime.strptime(date_str, fmt)
    # MiSeq (RTA 1x?)
    # 11/2/2017,03:08:24.972,Illumina RTA 1.18.54
    else:
        date_str_date = date_pad(data[0])
        date_str = date_str_date + " " + data[1]
        fmt = '%m/%d/%Y %H:%M:%S.%f'
        date_obj = datetime.datetime.strptime(date_str, fmt)
        version = data[2]
    return {"Date": date_obj, "Version": version}

def load_checkpoint(path):
    """Load the number and keyword from a Checkpoint.txt file, or None if not found."""
    try:
        with open(path) as fin:
            data = [line.strip() for line in fin]
    except FileNotFoundError:
        data = None
    else:
        data[0] = int(data[0])
    return data

def load_sample_filenames(dirpath):
    """Load a list of fastq.gz files from a directory and parse info from the filenames.

    The output is a sorted (by sample_num, read, and path) list of dictionaries
    with these string entries:

        prefix: the text corresponding to the sample name in the sample sheet
        sample_num: the sample number, as ordered in the sample sheet
        lane: sequencer lane for this sample.  Always 1 for MiSeq and MiniSeq.
        read: R1, R2, I1, or I2
        suffix: always 1
        path: absolute path to the file
    """
    path_attrs = []
    for path in Path(dirpath).glob("*.fastq.gz"):
        match = re.match(
            r"^(.+)_S([0-9]+)_L([0-9]{3})_(R1|R2|I1|I2)_([0-9]+)\.fastq\.gz$", path.name)
        if not match:
            continue
        fields = ["prefix", "sample_num", "lane", "read", "suffix"]
        attrs = dict(zip(fields, match.groups()))
        attrs["sample_num"] = int(attrs["sample_num"])
        attrs["lane"] = int(attrs["lane"])
        attrs["suffix"] = int(attrs["suffix"])
        attrs["path"] = str(path)
        path_attrs.append(attrs)
    path_attrs = sorted(path_attrs, key=lambda x: (x["sample_num"], x["read"], x["path"]))
    return path_attrs
