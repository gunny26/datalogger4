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
    project = "glt"
    tablename = "energiemonitor"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = "2016-04-05"
    tsa = TimeseriesArray(datalogger.index_keynames, datalogger.value_keynames, datatypes=datalogger.datatypes)
    tsa.debug = True
    for row in datalogger.raw_reader(datestring):
        logging.info(row)
        logging.info(float(row["wert"].replace(u",", u".")))
        tsa.add(row)
        unicode(row["bezeichnung"])
        if row["bezeichnung"] == u"Außentemperatur":
            logging.info("found")
        print(datetime.datetime.fromtimestamp(row["ts"]))
    #tsa = datalogger.load_tsa_raw(datestring)
    for ts in tsa.keys():
        print(ts)
    #cProfile.run("main()")
