#!/usr/bin/python
from __future__ import print_function
import cProfile
import sys
import gc
import datetime
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from datalogger import DataLogger as DataLogger

def main(project, tablename, datestring):
    datalogger = DataLogger(basedir, project, tablename)
    caches = datalogger.get_caches(datestring)
    suffix = "%s/%s/%s\t" % (datestring, project, tablename)
    data = None
    if caches["tsa"]["raw"] is None:
        print(suffix, "Nothing could be done without RAW data")
    else:
        if len(caches["tsa"]["keys"]) == 0:
            print(suffix, "TSA Archive missing, calling get_tsa and get_tsastats")
            data = datalogger.load_tsa(datestring)
        else:
            if len(caches["tsastat"]["keys"]) == 0:
                print(suffix, "TSASTAT Archive missing, calling get_tsastats")
                data = datalogger.load_tsastats(datestring)
            else:
                if len(caches["ts"]["keys"]) == 0:
                    print(suffix, "there are no ts archives, something went wrong, or tsa is completely empty, calling get_tsastats")
                    data = datalogger.load_tsa(datestring)
                else:
                    print(suffix, "All fine")
    del data
    del caches
    del datalogger
    #print(gc.get_count())

if __name__ == "__main__":
    basedir = "/var/rrd"
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    two_weeks_ago_daetstring = (datetime.date.today() - datetime.timedelta(28)).isoformat()
    for datestring in tuple(DataLogger.datewalker(two_weeks_ago_daetstring, yesterday_datestring)):
        for project in DataLogger.get_projects(basedir):
            for tablename in DataLogger.get_tablenames(basedir, project):
                main(project, tablename, datestring)
    #cProfile.run("main()")
