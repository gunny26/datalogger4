#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import datetime
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from datalogger import DataLogger as DataLogger
#from commons import *

def main(project, tablename, datestring):
    #caches = datalogger.get_caches(datestring)
    datalogger = DataLogger(basedir, project, tablename)
    caches = datalogger.get_caches(datestring)
    suffix = "%s/%s/%s\t" % (datestring, project, tablename)
    if caches["tsa"]["raw"] is None:
        print(suffix, "Nothing could be done without RAW data")
    else:
        #print("RAW filename : %s" % caches["tsa"]["raw"])
        if len(caches["tsa"]["keys"]) == 0:
            print(suffix, "TSA Archive missing, calling get_tsa and get_tsastats")
            #datalogger.get_tsa(project, tablename, datestring)
            datalogger.load_tsastats(datestring)
        else:
            #print("TSA filename : %s" % caches["tsa"]["keys"])
            if len(caches["tsastat"]["keys"]) == 0:
                print(suffix, "TSASTAT Archive missing, calling get_tsastats")
                datalogger.load_tsastats(datestring)
            else:
                #print("TSASTAT filename : %s" % caches["tsastat"]["keys"])
                if len(caches["ts"]["keys"]) == 0:
                    print(suffix, "there are no ts archives, something went wrong, or tsa is completely empty, calling get_tsastats")
                    datalogger.load_tsastats(datestring)
                else:
                    #print("TS filename : %s" % len(caches["ts"]["keys"]))
                    #print("TSSTAT filename : %s" % len(caches["tsstat"]["keys"]))
                    print(suffix, "All fine")

if __name__ == "__main__":
    basedir = "/var/rrd"
    #for datestring in DataLogger.datewalker("2015-09-01", datalogger.get_last_business_day_datestring()):
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    two_weeks_ago_daetstring = (datetime.date.today() - datetime.timedelta(28)).isoformat()
    for datestring in tuple(DataLogger.datewalker(two_weeks_ago_daetstring, yesterday_datestring)):
        for project in DataLogger.get_projects(basedir):
            for tablename in DataLogger.get_tablenames(basedir, project):
                #datalogger = DataLogger(BASEDIR, project, tablename)
                main(project, tablename, datestring)
    #cProfile.run("main()")
