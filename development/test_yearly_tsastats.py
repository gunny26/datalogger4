#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import os
import json
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArrayLazy as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import Timeseries as Timeseries
from tk_vch import VirtualCenterHelperWeb as VirtualCenterHelperWeb
#from commons import *

def sr_report_unused_cpu(startdate, enddate):
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger("/var/rrd", project, tablename)
    print("datestring\t#vpus\t#vms\tidle_avg\tused_avg\tused_sum\tvcpus_per_vm")
    for datestring in DataLogger.datewalker(startdate, enddate):
        tsastat = datalogger.load_tsastats(datestring)
        tsastat_h = datalogger.tsastat_group_by(tsastat, ("hostname", ))
        vmcount = len(tsastat_h.keys())
        tsastat_g = datalogger.tsastat_group_by(tsastat, ())
        for key in tsastat_g.keys():
            print("%s\t%s\t%s\t%0.2f\t%0.2f\t%0.2f\t%0.2f" % (datestring, len(tsastat.keys()), vmcount, tsastat_g[key]["cpu.idle.summation"]["avg"], tsastat_g[key]["cpu.used.summation"]["avg"], tsastat_g[key]["cpu.used.summation"]["std"], float(len(tsastat.keys())) / vmcount))


def sr_report_mem(startdate, enddate):
    print("datetime\t#vms\tmem_active_avg\tmem_granted_avg\ttotal_granted_mem")
    for datestring in DataLogger.datewalker(startdate, enddate):
        tsastat = datalogger.load_tsastats(datestring)
        tsastat_g = datalogger.tsastat_group_by(tsastat, ())
        for key in tsastat_g.keys():
            print("%s\t%s\t%00.2f\t%00.2f\t%00.2f" % (datestring, len(tsastat.keys()), tsastat_g[key]["mem.active.average"]["avg"], tsastat_g[key]["mem.granted.average"]["avg"], len(tsastat.keys()) * tsastat_g[key]["mem.granted.average"]["avg"]))

def sr_report_unused_vhosts(startdate, enddate):
    project = "haproxy"
    tablename = "http_host"
    datalogger = DataLogger("/var/rrd", project, tablename)
    for datestring in DataLogger.datewalker(startdate, enddate):
        tsastat = datalogger.load_tsastats(datestring)
        tsastat_g = datalogger.tsastat_group_by(tsastat, ())
        for key in tsastat_g.keys():
            print("%s\t%0.2f\t%0.2f\t%0.2f" % (datestring, tsastat_g[key]["hits"]["avg"], tsastat_g[key]["hits"]["sum"], tsastat_g[key]["bytes_read"]["sum"]))


if __name__ == "__main__":
    project = "vicenter"
    tablename = "virtualMachineMemoryStats"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = DataLogger.get_last_business_day_datestring()
    # VirtualCenterHelperWeb setup
    os.environ['NO_PROXY'] = 'tirol-kliniken.cc'
    URL = "http://sbapps.tirol-kliniken.cc/vsphere"
    APIKEY = "05faa0ca-038f-49fd-b4e9-6907ccd06a1f"
    webapp = VirtualCenterHelperWeb(URL, APIKEY)
    #sr_report_unused_vhosts("2016-01-01", datestring)
    #sr_report_unused_cpu("2015-09-01", datestring)
    sr_report_mem("2015-09-01", datestring)
    #main()
    #cProfile.run("main()")
