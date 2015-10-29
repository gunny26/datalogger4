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

def main(datestring, datalogger):
    caches = datalogger.get_caches(datestring)
    suffix = "%s/%s/%s\t" % (datestring, project, tablename)
    if caches["tsa"]["raw"] is None:
        print(suffix, "Nothing could be done without RAW data")
    else:
        #print("RAW filename : %s" % caches["tsa"]["raw"])
        if len(caches["tsa"]["keys"]) == 0:
            print(suffix, "TSA Archive should be created")
            datalogger.load_tsa(datestring)
        else:
            #print("TSA filename : %s" % caches["tsa"]["keys"])
            if len(caches["tsastat"]["keys"]) == 0:
                print(suffix, "TSASTAT Archive missing")
                datalogger.load_tsastats(datestring)
            else:
                #print("TSASTAT filename : %s" % caches["tsastat"]["keys"])
                if len(caches["ts"]["keys"]) == 0:
                    print(suffix, "there are no ts archives, something went wrong, or tsa is completely empty")
                else:
                    #print("TS filename : %s" % len(caches["ts"]["keys"]))
                    #print("TSSTAT filename : %s" % len(caches["tsstat"]["keys"]))
                    print(suffix, "All fine")

if __name__ == "__main__":
    for datestring in DataLogger.datewalker("2015-10-01", DataLogger.get_last_business_day_datestring()):
        for project in DataLogger.get_projects(BASEDIR):
            for tablename in DataLogger.get_tablenames(BASEDIR, project):
                datalogger = DataLogger(BASEDIR, project, tablename)
                main(datestring, datalogger)
    #cProfile.run("main()")
