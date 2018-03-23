#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import datetime
import json
import logging
logging.basicConfig(level=logging.INFO)
import argparse
# own modules
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="to read from raw data and filter")
    parser.add_argument("-p", "--project", help="project to read from", required=True)
    parser.add_argument("-t", "--tablename", help="tablename in project to read from", required=True)
    parser.add_argument("-d", "--datestring", help="datestring to use", required=True)
    parser.add_argument("--value-keys", help="value_key to filter for")
    parser.add_argument("--index-pattern", help="pattern to filter index_key for (used with in)")
    options = parser.parse_args("-p ucs -t ifXTable -d 2017-11-29 --value-keys=ifHCOutOctets --index-pattern=ucsfia-sr1-2-mgmt0".split())
    datalogger = DataLogger("/var/rrd", options.project, options.tablename)
    value_keys = datalogger.value_keynames
    if options.value_keys is not None:
        value_keys = [item.strip() for item in options.value_keys.split(",")]
    try:
        assert all((value_key in datalogger.value_keynames for value_key in value_keys))
    except AssertionError:
        print("some value_keynames in %s are unknown" % value_keys)
        sys.exit(1)
    print("showing only value_keynames : %s" % value_keys)
    print(json.dumps(datalogger.headers, indent=4))
    tsa = TimeseriesArray(datalogger.index_keynames, datalogger.value_keynames, datatypes=datalogger.datatypes)
    tsa.debug = True
    num_keys = None
    firstrow = True
    for row in datalogger.raw_reader(options.datestring):
        if firstrow is True:
            print("\t".join(datalogger.headers))
            firstrow = False
        index_str = str([row[key] for key in datalogger.index_keynames])
        if options.index_pattern is not None and options.index_pattern not in index_str:
            continue
        value_str = "\t".join([row[key] for key in value_keys])
        print("%s : %s : %s" % (datetime.datetime.fromtimestamp(row["ts"]), index_str, value_str))
        if num_keys is None:
            num_keys = len(row.keys())
        assert num_keys == len(row.keys())
        #tsa.add(row)
