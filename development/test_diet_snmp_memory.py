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
    data = []
    data.append(("hostname", "hrStorageSizeKb", "hrStorageUsedKb", "hrStorageNotUsedKbMin"))
    for index_key in tsastat.keys():
        # (u'srvcacdbp1.tilak.cc', u'Physical Memory',
        # u'HOST-RESOURCES-TYPES::hrStorageRam')
        if u'HOST-RESOURCES-TYPES::hrStorageRam' not in index_key:
            del tsastat[index_key]
    for key, tsstat in datalogger.tsastat_group_by(tsastat, ("hostname", )).items():
        sizekb = tsstat["hrStorageSize"]["min"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
        usedkb = tsstat["hrStorageUsed"]["max"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
        notused = sizekb - usedkb
        data.append((key[0], "%0.2f" % sizekb, "%0.2f" % usedkb, "%0.2f" % notused))
    for row in data:
        print("\t".join(row))

if __name__ == "__main__":
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = DataLogger.get_last_business_day_datestring()
    #main()
    cProfile.run("main()")
