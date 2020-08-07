#!/usr/bin/env python

import sys
import warnings
import requests
import yaml

def get_status(itemname=None):
    if itemname:
        response = requests.get("http://127.0.0.1:8888/status/" + itemname)
    else:
        response = requests.get("http://127.0.0.1:8888/status")
    # whatever happens server-side, the response should be YAML.
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        data = yaml.safe_load(response.text)
    return data

def format_status(status_info):
    print(status_info)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        status_info = get_status(sys.argv[1])
    else:
        status_info = get_status()
    format_status(status_info)
