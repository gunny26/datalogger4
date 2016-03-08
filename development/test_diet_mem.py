#!/usr/bin/python -3
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import Timeseries as Timeseries

def main():
    tsastat = datalogger.load_tsastats("2016-02-08")
    tsastat_g = datalogger.tsastat_group_by(tsastat, ("hostname", ))
    data = []
    data.append(("hostname", "avg_active_max", "avg_granted_min", "avg_notused_min"))
    for key in tsastat_g.keys():
        #print("%s : %s" % (key[0], tsastat_g[key]["cpu.idle.summation"]["min"]))
        not_used = tsastat_g[key]["mem.granted.average"]["min"] - tsastat_g[key]["mem.active.average"]["max"]
        data.append((key[0], str(tsastat_g[key]["mem.active.average"]["max"]), str(tsastat_g[key]["mem.granted.average"]["min"]), str(not_used)))
    for row in data:
        print("\t".join(row))

if __name__ == "__main__":
    project = "vicenter"
    tablename = "virtualMachineMemoryStats"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = DataLogger.get_last_business_day_datestring()
    #main()
    cProfile.run("main()")
