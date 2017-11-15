#!/usr/bin/python
"""
Tool to analyze some datalogger raw data
"""
from __future__ import print_function
import os
import sys
import argparse
import json

parser = argparse.ArgumentParser(description="Tool to analyze some datalogger raw data")
parser.add_argument("-i", "--input-file", help="file to read from", required=True)
options = parser.parse_args("-i /var/rrd/snmp/raw/ifTable_2017-11-15.csv".split())
if not os.path.isfile(options.input_file):
    print("file %s does not exist" % options.input_file)
    sys.exit(1)
data = {}
meta = {}
meta["delimiter"] = "\t"
meta["index_keynames"] = ("hostname", "ifDescr")
meta["ts_keyname"] = "ts"
meta["interval"] = 300
headers = None
with open(options.input_file, "rt") as infile:
    for line in infile.read().split("\n"):
        if line == "" or line == "\n":
            continue
        if headers is None:
            headers = line.split(meta["delimiter"])
            meta["headers"] = headers
            data["length"] = len(headers)
            for header in headers:
                data[header] = {
                    "isnumeric" : True,
                    "interval" : 0
                    }
            assert meta["ts_keyname"] in headers
            assert all((index_key in headers for index_key in meta["index_keynames"]))
        else:
            columns = line.split(meta["delimiter"])
            assert len(columns) == data["length"]
            for index, column in enumerate(columns):
                data[headers[index]]["isnumeric"] = all((data[headers[index]]["isnumeric"], column.isnumeric()))
        print(line)
meta["value_keynames"] = dict([(header, "asis") for header in headers if data[header]["isnumeric"] == True])
meta["blacklist"] = [header for header in headers if (data[header]["isnumeric"] == False) and (header not in meta["index_keynames"]) and (header != meta["ts_keyname"])]
print(json.dumps(meta, indent=4, sort_keys=True))
