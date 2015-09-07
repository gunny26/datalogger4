#!/usr/bin/python
import cProfile
import logging
logging.basicConfig(level=logging.DEBUG)
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import TimeseriesArray as TimeseriesArray
from commons import *

def calc_hrStorageSizeUsage(data):
    try:
        return 100 * data[u"hrStorageUsed"] / data[u"hrStorageSize"]
    except ZeroDivisionError:
        return(-1)

def calc_hrStorageSizeKb(data):
    try:
        return data[u"hrStorageSize"] * data[u"hrStorageAllocationUnits"] / 1024
    except ZeroDivisionError:
        return(-1)

def calc_hrStorageUsedKb(data):
    try:
        return data[u"hrStorageUsed"] * data[u"hrStorageAllocationUnits"] / 1024
    except ZeroDivisionError:
        return(-1)

def calc_hrStorageFreeKb(data):
    try:
        return (data[u"hrStorageSize"] - data[u"hrStorageUsed"]) * data[u"hrStorageAllocationUnits"] / 1024
    except ZeroDivisionError:
        return(-1)

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
    ts_keyname = tsa.get_ts_key()
    for row in tsa.dump():
        row[ts_keyname] = align_timestamp(row[ts_keyname])
        yield row

def read_tsa_full_aligned(datestring, slotlength):
    tsa = datalogger.read_tsa_full(datestring, force=True)
    print tsa.index_keys, tsa.value_keys, tsa.ts_key
    tsa2 = TimeseriesArray(tuple(tsa.index_keys), list(tsa.value_keys), str(tsa.ts_key))
    print "old times"
    print sorted(tsa[tsa.keys()[0]].get_times())
    for data in dump_and_align(tsa, slotlength):
        tsa2.add(data)
    print "new times"
    print sorted(tsa2[tsa2.keys()[0]].get_times())
    assert tsa != tsa2
    assert all(key in tsa2.keys() for key in tsa.keys())
    assert len(tsa) == len(tsa2)
    return tsa2

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = read_tsa_full_aligned(datestring, 600)
    keys = tsa.keys()
    for slottime in get_slot_timeline(datestring, 600):
        print slottime, slottime in tsa[keys[0]].get_times()
        #print tuple((tsa[key].get_single_value(slottime, 'hrStorageAllocationFailures') for key in keys))

    return


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
