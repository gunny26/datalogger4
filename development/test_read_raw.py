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
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
#from commons import *

if __name__ == "__main__":
    project = "snmp"
    tablename = "ifXTable"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = "2017-11-13"
    tsa = TimeseriesArray(datalogger.index_keynames, datalogger.value_keynames, datatypes=datalogger.datatypes)
    tsa.debug = True
    num_keys = None
    for row in datalogger.raw_reader(datestring):
        logging.info(row)
        if num_keys is None:
            num_keys = len(row.keys())
        assert num_keys == len(row.keys())
        #tsa.add(row)
        print(datetime.datetime.fromtimestamp(row["ts"]))
    #tsa = datalogger.load_tsa_raw(datestring)
#    for ts in tsa.keys():
#        print(ts)
    #cProfile.run("main()")
