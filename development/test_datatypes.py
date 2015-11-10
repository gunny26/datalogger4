#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import time
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import Timeseries as Timeseries
from commons import *

datatypes = {
    "fcIfC3Discards" : "persecond",
    "fcIfC3InFrames" : "persecond",
    "fcIfC3InOctets" : "persecond",
    "fcIfC3OutFrames" : "persecond",
    "fcIfC3OutOctets" : "persecond",
    "index" : "asis",
}

def read_tsa_full_aligned(datestring, slotlength):
    """
    align, timestamp fileds to given timeline with predefined slotlength
    used to aggregate this data afterwards
    """
    #testcol = u"fcIfC3InOctets"
    starttime = time.time()
    tsa = datalogger[datestring]
    print(datalogger.datatypes)
    print("Duration Load %f" % (time.time() - starttime))
    startime = time.time()
    # strip down to only one timeseries
    #for key in tsa.keys()[1:]:
    #    del tsa[key]
    #key = tsa.keys()[0]
    #print(key)
    #print(tsa[key].colnames)
    #print(tsa[key].headers)
    # get rid of not interesting columns#
    #headers_to_remove = []
    for colname, datatype in datatypes.items():
        if datatype != "asis":
            tsa.convert(colname, datatype)
    print("Duration Convert %f" % (time.time() - starttime))
    startime = time.time()
    print(tsa[tsa.keys()[0]])
    tsastats = tsa.stats
    print(tsastats)


def main():
    read_tsa_full_aligned(datestring, slotlength=600)

if __name__ == "__main__":
    project = "sanportperf"
    tablename = "fcIfC3AccountingTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    #main()
    cProfile.run("main()")
