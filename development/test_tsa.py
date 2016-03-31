#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import logging
logging.basicConfig(level=logging.DEBUG)
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArrayLazy as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import Timeseries as Timeseries
#from commons import *

def main():
    tsa = datalogger["2016-02-08"]
    tsa.set_group_keyname("hostname", lambda a, b: a + b)
    for key in tsa.keys():
        tsa.getitem_grouped__(key)
        #print(key, tsa[key])

if __name__ == "__main__":
    project = "ucs"
    tablename = "ifXTable"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = DataLogger.get_last_business_day_datestring()
    main()
    cProfile.run("main()")
