"""
Utility functions used throughout the package.

These are largely just wrappers for filesystem operations or text manipulation.
"""

import struct
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

def load_csv(path, loader=csv.reader, non_unicode=None):
    """Load CSV data from a given file path.

    By default returns a list of lists using csv.reader, but another reader
    than can operate on a file object (e.g. csv.DictReader) can be supplied
    instead.  Supports UTF8 (with or without a byte order mark) and
    equivalently ASCII.

    The behavior for non-unicode characters is controlled by the non_unicode
    argument.  If None (default), no special handling is provided so a Unicode
    parsing exception would be raised.  If "replace", every instance of a
    non-unicode character is replaced with unicode's placeholder "replacement
    character" U+FFFD.  If "strip", the replacement is performed first and then
    all replacement characters are removed.  (Note that this means any
    replacement characters already there will *also* be removed, but it's an
    edge case to an edge case.)
    """

    # Set up the handling for non-unicode text.  In most cases we don't change
    # the text so there's a no-op lambda function.  Only in the "strip" case
    # does mapfunc do something.
    mapfunc = lambda _: _
    if non_unicode == "replace":
        errors_mode = "replace"
    elif non_unicode is None or non_unicode == "strict":
        errors_mode = "strict"
    elif non_unicode == "strip":
        errors_mode = "replace"
        mapfunc = lambda x: re.sub("\N{REPLACEMENT CHARACTER}", "", x)
    else:
        raise ValueError('non_unicode should be one of None, "replace", "mask"')
    # Explicitly setting the encoding to utf-8-sig allows the byte order mark
    # to be automatically stripped out if present.  Anything that's non-unicode
    # will be handled as defined in the errors argument, set up above.
    with open(path, 'r', newline='', encoding='utf-8-sig', errors=errors_mode) as fin:
        # mapfunc will alter the text during the iteration, but only if the
        # "strip" option was given.
        data = list(loader(map(mapfunc, fin)))
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
            if len(row) == 1:
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
        samples.append(dict(zip(cols, row)))
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

# https://support.illumina.com/content/dam/illumina-support/documents/documentation/software_documentation/bcl2fastq/bcl2fastq_letterbooklet_15038058brpmi.pdf
#
# Start    Description                                                    Data type
# Byte 0   Cycle number                                                   integer
# Byte 4   Average Cycle Intensity                                        double
# Byte 12  Average intensity for A over all clusters with intensity for A double
# Byte 20  Average intensity for C over all clusters with intensity for C double
# Byte 28  Average intensity for G over all clusters with intensity for G double
# Byte 36  Average intensity for T over all clusters with intensity for T double
# Byte 44  Average intensity for A over clusters with base call A         double
# Byte 52  Average intensity for C over clusters with base call C         double
# Byte 60  Average intensity for G over clusters with base call G         double
# Byte 68  Average intensity for T over clusters with base call T         double
# Byte 76  Number of clusters with base call A                            integer
# Byte 80  Number of clusters with base call C                            integer
# Byte 84  Number of clusters with base call G                            integer
# Byte 88  Number of clusters with base call T                            integer
# Byte 92  Number of clusters with base call X                            integer
# Byte 96  Number of clusters with intensity for A                        integer
# Byte 100 Number of clusters with intensity for C                        integer
# Byte 104 Number of clusters with intensity for G                        integer
# Byte 108 Number of clusters with intensity for T                        integer
def load_bcl_stats(path):
    """Load a single BCL stats file into a dictionary.

    See Illumina's bcl2fastq documentation for details on the values.  Note
    that cycle number here is zero-indexed while in the directory names it's
    indexed by one.
    """
    fmt = "<IdddddddddIIIIIIIII"
    with open(path, "rb") as f_in:
        raw = f_in.read(112)
    data = struct.unpack(fmt, raw)
    keys = ["cycle", "avg_intensity"] + \
        ["avg_int_all_%s" % base for base in ["A", "C", "G", "T"]] + \
        ["avg_int_cluster_%s" % base for base in ["A", "C", "G", "T"]] + \
        ["num_clust_call_%s" % base for base in ["A", "C", "G", "T", "X"]] + \
        ["num_clust_int_%s" % base for base in ["A", "C", "G", "T"]]
    data = dict(zip(keys, data))
    return data
