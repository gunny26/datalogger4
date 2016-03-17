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
    data.append(("hostname", "avg_idle_min", "avg_used_avg", "avg_used_max"))
    for key in tsastat_g.keys():
        num_cpu = sum([key[0] in index_key for index_key in tsastat.keys()])
        if num_cpu < 3 :
            continue
        #print("%s : %s" % (key[0], tsastat_g[key]["cpu.idle.summation"]["min"]))
        data.append((key[0], str(tsastat_g[key]["cpu.idle.summation"]["min"]), str(tsastat_g[key]["cpu.used.summation"]["avg"]), str(tsastat_g[key]["cpu.used.summation"]["max"])))
    for row in data:
        print("\t".join(row))

if __name__ == "__main__":
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = DataLogger.get_last_business_day_datestring()
    #main()
    cProfile.run("main()")
