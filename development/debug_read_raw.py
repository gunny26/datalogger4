#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import datetime
import logging
logging.basicConfig(level=logging.DEBUG)
import argparse
# own modules
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="to read from raw data and filter")
    parser.add_argument("-p", "--project", help="project to read from", required=True)
    parser.add_argument("-t", "--tablename", help="tablename in project to read from", required=True)
    parser.add_argument("-d", "--datestring", help="datestring to use", required=True)
    parser.add_argument("--value-key", help="value_key to filter for")
    options = parser.parse_args("-p snmp -t ifTable -d 2017-11-27".split())
    datalogger = DataLogger("/var/rrd", options.project, options.tablename)
    tsa = TimeseriesArray(datalogger.index_keynames, datalogger.value_keynames, datatypes=datalogger.datatypes)
    tsa.debug = True
    num_keys = None
    for row in datalogger.raw_reader(options.datestring):
        logging.info(row)
        if num_keys is None:
            num_keys = len(row.keys())
        assert num_keys == len(row.keys())
        #tsa.add(row)
        print(datetime.datetime.fromtimestamp(row["ts"]))
