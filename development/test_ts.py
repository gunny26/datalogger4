#!/usr/bin/python
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
from commons import *

def read_tsa_full_aligned(datestring, slotlength):
    """
    align, timestamp fileds to given timeline with predefined slotlength
    used to aggregate this data afterwards
    """
    testcol = u"fcIfC3InOctets"
    tsa = datalogger[datestring]
    # strip down to only one timeseries
    for key in tsa.keys()[1:]:
        del tsa[key]
    key = tsa.keys()[0]
    print(key)
    print(tsa[key].colnames)
    print(tsa[key].headers)
    # get rid of not interesting columns#
    headers_to_remove = []
    for header in tsa[key].headers:
        print(header)
        if header != testcol:
            headers_to_remove.append(header)
    for header in headers_to_remove:
        tsa[key].remove_col(header)
    print(tsa[key])
    tsa[key].convert(testcol, "derive", "%s_d" % testcol)
    print(tsa[key])
    tsa[key].convert("%s_d" % testcol, "derive", "%s_dd" % testcol)
    print(tsa[key])
    tsa[key].convert(testcol, "percent", "%s_pct" % testcol)
    print(tsa[key])
    tsa[key].convert(testcol, "persecond", "%s_ps" % testcol)
    print(tsa[key])
    tsa[key].convert(testcol, "counter32", "%s_c32" % testcol)
    print(tsa[key])
    tsa[key].convert(testcol, "counter64", "%s_c64" % testcol)
    print(tsa[key])
    tsa[key].convert(testcol, "counterreset", "%s_cr" % testcol)
    print(tsa[key])
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
