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
from commons import *

def get_slot_timeline(datestring, slotlength):
    start_ts, stop_ts = datalogger.get_ts_for_datestring(datestring)
    timeline = []
    while start_ts < stop_ts:
        timeline.append(start_ts)
        start_ts += slotlength
    return tuple(timeline)

def gen_align_timestamp(datestring, slotlength):
    """
    generator for align_timestamp
    performance boost about 100% over old fashioned way
    """
    start_ts, stop_ts = datalogger.get_ts_for_datestring(datestring)
    def align_timestamp(timestamp):
        nearest_slot = round((timestamp-start_ts) / slotlength)
        nearest_timestamp = start_ts + nearest_slot * slotlength
        return int(nearest_timestamp)
    return align_timestamp

def dump_and_align(tsa, slotlength):
    """
    dump timeseries with aligned timestamps
    """
    align_timestamp = gen_align_timestamp(datestring, slotlength)
    ts_keyname = tsa.ts_key
    for row in tsa.export():
        before = row[ts_keyname]
        newrow = copy.copy(row)
        newrow[ts_keyname] = align_timestamp(before)
        #print "correcting %f -> %f" % (before, row[ts_keyname])
        yield newrow

def read_tsa_full_aligned(datestring, slotlength):
    """
    align, timestamp fileds to given timeline with predefined slotlength
    used to aggregate this data afterwards
    """
    tsa = datalogger.load_tsa(datestring)
    key = tsa.keys()[0]
    tsa2 = TimeseriesArray(tsa.index_keys, tsa.value_keys, tsa.ts_key)
    print(key)
    print("old times")
    print(tsa[key])
    for data in dump_and_align(tsa, slotlength):
        tsa2.add(data)
    print("new times")
    print(gc.get_referrers(tsa))
    print(gc.get_referrers(tsa2))
    print(tsa2[key]["ts"])
    assert tsa[key] == tsa2[key]
    assert all(key in tsa2.keys() for key in tsa.keys())
    assert len(tsa) == len(tsa2)
    return tsa2

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = read_tsa_full_aligned(datestring, 600)
    #keys = tsa.keys()
    #for slottime in get_slot_timeline(datestring, 600):
    #    print(slottime)
        #print tuple((tsa[key].get_single_value(slottime, 'hrStorageAllocationFailures') for key in keys))
    #return


def main():
    report(datalogger, datestring)
    #report_ram(datalogger, datestring)

if __name__ == "__main__":
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    #main()
    cProfile.run("main()")
