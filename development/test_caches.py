#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import Timeseries as Timeseries
from commons import *

def main():
    caches = datalogger.get_caches(datestring)
    print("RAW filename : %s" % caches["tsa"]["raw"])
    print("TSA filename : %s" % caches["tsa"]["keys"])
    print("TS filename : %s" % len(caches["ts"]["keys"]))
    print("TSASTAT filename : %s" % caches["tsastat"]["keys"])
    print("TSSTAT filename : %s" % len(caches["tsstat"]["keys"]))

if __name__ == "__main__":
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    main()
    #cProfile.run("main()")
