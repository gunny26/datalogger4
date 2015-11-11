#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from datalogger import DataLoggerWeb as DataLoggerWeb
#from datalogger import TimeseriesArray as TimeseriesArray
#from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
#from datalogger import Timeseries as Timeseries
from commons import *

def main(project, tablename, datestring):
    caches = dataloggerweb.get_caches(project, tablename, datestring)
    #print(caches)
    tsa = dataloggerweb.get_tsa(project, tablename, datestring)
    # filtering
    blacklist = ("10.3.51.11", "10.3.23.99", )
    for key in tsa.keys():
        for item in blacklist:
            if item in key:
                print("deleting key %s" % str(key))
                try:
                    del(tsa[key])
                except KeyError:
                    pass
    tsa = datalogger.group_by(datestring, tsa, ("http_host",), group_func=lambda a,b: a + b)
    tsastats = TimeseriesArrayStats(tsa)
    for key, data in sorted(tsastats.items(), key=lambda a: a[1]["hits"]["sum"], reverse=True):
        try:
            print("%050s : %10d" % (key[0], data["hits"]["sum"]))
        except KeyError:
            print(key[0], " empty")
            pass
    return
    print(tsa[tsa.keys()[0]])
    tsastats = dataloggerweb.load_tsastats(project, tablename, datestring)
    print(tsastats[tsastats.keys()[0]])

if __name__ == "__main__":
    project = "haproxy"
    tablename = "haproxylog"
    datalogger = DataLogger(BASEDIR, project, tablename)
    dataloggerweb = DataLoggerWeb(DATALOGGER_URL)
    main(project, tablename, "2015-11-03")
    #cProfile.run("main()")
