#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import time
import json
import logging
logging.basicConfig(level=logging.DEBUG)
# own modules
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import Timeseries as Timeseries
from commons import *

def main1():
    meta = json.load(open("/var/rrd/ucs/meta/ifTable.json"))
    print(meta)
    #tsa = datalogger[datestring]
    for line in open("/var/rrd/ucs/raw/ifTable_2015-10-28.csv", "rb").read().split("\n")[1:]:
        print(line)
        data = dict(zip(meta["headers"], line.split("\t")))
        for index, key in enumerate(meta["headers"]):
            if key in meta["value_keynames"]:
                float(data[key])
            print("%s = %s = %s" % (meta["headers"][index], line.split("\t")[index], data[key]))
        print(data)

def main2():
    tsa = datalogger.load_tsa_raw(datestring)
    print(len(tsa.keys()))
    print(tsa.keys()[0])
    print(tsa[tsa.keys()[0]])
    tsastat = TimeseriesArrayStats(tsa)
    print(tsastat[tsastat.keys()[0]])

def main3():
    tsa = datalogger[datestring]
    print(len(tsa.keys()))
    print(tsa.keys()[0])
    print(tsa[tsa.keys()[0]])
    tsastat = TimeseriesArrayStats(tsa)
    print(tsastat[tsastat.keys()[0]])
    print(tsa[("ucsfib-sr1-1-mgmt0","port-channel1304","propVirtual")]["ifOutOctets"])
    print(tsastat[("ucsfib-sr1-1-mgmt0","port-channel1304","propVirtual")])


if __name__ == "__main__":
    project = "ucs"
    tablename = "ifTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = "2015-10-28"
    #main()
    cProfile.run("main3()")
