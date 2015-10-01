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
from datalogger import Timeseries as Timeseries
from commons import *

#def __get_slot_timeline(datestring, slotlength):
#    start_ts, stop_ts = datalogger.get_ts_for_datestring(datestring)
#    start_ts = start_ts
#    stop_ts = stop_ts
#    timeline = []
#    while start_ts < stop_ts:
#        timeline.append(start_ts)
#        start_ts += slotlength
#    return tuple(timeline)

#def __gen_align_timestamp(datestring, slotlength):
#    """
#    generator for align_timestamp
#    performance boost about 100% over old fashioned way
#    """
#    start_ts, stop_ts = datalogger.get_ts_for_datestring(datestring)
#    def align_timestamp(timestamp):
#        nearest_slot = round((timestamp - start_ts) / slotlength)
#        nearest_timestamp = int(start_ts + nearest_slot * slotlength)
#        return nearest_timestamp
#    return align_timestamp

#def __dump_and_align(tsa, slotlength):
#    """
#    dump timeseries with aligned timestamps
#    """
#    align_timestamp = __gen_align_timestamp(datestring, slotlength)
#    ts_keyname = tsa.ts_key
#    for data in tsa.export():
#        # correct timestamp
#        data[ts_keyname] = align_timestamp(data[ts_keyname])
#        yield data

def group_by_local(tsa, datestring, subkeys, group_func, slotlength):
    """
    group given tsa by subkeys, and use group_func to aggregate data
    first all Timeseries will be aligned in time, got get proper points in time

    parameters:
    tsa <TimeseriesArray>
    subkey <tuple> could also be empty, to aggregate everything
    group_func <func>
    slotlength <int>

    returns:
    <TimeseriesArray>
    """
    starttime = time.time()
    # intermediated tsa
    tsa2 = TimeseriesArray(index_keys=subkeys, value_keys=tsa.value_keys, ts_key=tsa.ts_key)
    start_ts, stop_ts = datalogger.get_ts_for_datestring(datestring)
    #align_timestamp = __gen_align_timestamp(datestring, slotlength)
    ts_keyname = tsa.ts_key
    for data in tsa.export():
        # align timestamo
        nearest_slot = round((data[ts_keyname] - start_ts) / slotlength)
        data[ts_keyname] = int(start_ts + nearest_slot * slotlength)
        #data[ts_keyname] = align_timestamp(data[ts_keyname])
        tsa2.group_add(data, group_func)
    print("Duration : %f" % (time.time() - starttime))
    #print("standardized Timeseries")
    #print(tsa2[tsa2.keys()[0]])
    #print("Grouping into one single timeseries")
    # group by hostname
    #tsa3 = TimeseriesArray(index_keys=subkeys, value_keys=tsa.value_keys, ts_key=tsa.ts_key)
    #for data in tsa2.export():
    #    tsa3.group_add(data, group_func)
    return tsa2

def read_tsa_full_aligned(datestring, slotlength):
    """
    align, timestamp fileds to given timeline with predefined slotlength
    used to aggregate this data afterwards
    """
    tsa = datalogger.load_tsa(datestring)
    # strip down to only one timeseries
    #for key in tsa.keys()[1:]:
    #    del tsa[key]
    key = tsa.keys()[0]
    print(key)
    print("original Timeseries")
    print(tsa[key])
    tsa3 = datalogger.group_by(datestring, tsa, (), lambda a,b: (a + b) / 2, slotlength)
    print(tsa3.keys()[0])
    print(tsa3[tsa3.keys()[0]])

def main():
    read_tsa_full_aligned(datestring, slotlength=600)

if __name__ == "__main__":
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    main()
    #cProfile.run("main()")
