#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from datalogger import DataLoggerWeb as DataLoggerWeb
from commons import *

def main(project, tablename, datestring, datalogger):
    #caches = datalogger.get_caches(datestring)
    caches = datalogger.get_caches(project, tablename, datestring)
    suffix = "%s/%s/%s\t" % (datestring, project, tablename)
    if caches["tsa"]["raw"] is None:
        print(suffix, "Nothing could be done without RAW data")
    else:
        #print("RAW filename : %s" % caches["tsa"]["raw"])
        if len(caches["tsa"]["keys"]) == 0:
            print(suffix, "TSA Archive missing, calling get_tsa and get_tsastats")
            #datalogger.get_tsa(project, tablename, datestring)
            datalogger.get_tsastats(project, tablename, datestring)
        else:
            #print("TSA filename : %s" % caches["tsa"]["keys"])
            if len(caches["tsastat"]["keys"]) == 0:
                print(suffix, "TSASTAT Archive missing, calling get_tsastats")
                datalogger.get_tsastats(project, tablename, datestring)
            else:
                #print("TSASTAT filename : %s" % caches["tsastat"]["keys"])
                if len(caches["ts"]["keys"]) == 0:
                    print(suffix, "there are no ts archives, something went wrong, or tsa is completely empty, calling get_tsastats")
                    datalogger.get_tsastats(project, tablename, datestring)
                else:
                    #print("TS filename : %s" % len(caches["ts"]["keys"]))
                    #print("TSSTAT filename : %s" % len(caches["tsstat"]["keys"]))
                    print(suffix, "All fine")

if __name__ == "__main__":
    datalogger = DataLoggerWeb()
    #for datestring in DataLogger.datewalker("2015-09-01", datalogger.get_last_business_day_datestring()):
    for datestring in datalogger.get_datewalk("2015-11-01", datalogger.get_last_business_day_datestring()):
        for project in datalogger.get_projects():
            for tablename in datalogger.get_tablenames(project):
                #datalogger = DataLogger(BASEDIR, project, tablename)
                main(project, tablename, datestring, datalogger)
    #cProfile.run("main()")
