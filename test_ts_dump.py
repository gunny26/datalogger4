#!/usr/bin/python
import cProfile
import logging
logging.basicConfig(level=logging.INFO)
import gzip
import cPickle
import time
# won modules
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import TimeseriesArray as TimeseriesArray
from tilak_datalogger import Timeseries as Timeseries
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
    tsa = datalogger.read_tsa_full(datestring, force=False)
    print tsa.index_keys, tsa.value_keys, tsa.ts_key
    tsa2 = TimeseriesArray(tuple(tsa.index_keys), list(tsa.value_keys), str(tsa.ts_key))
    for data in dump_and_align(tsa, slotlength):
        tsa2.add(data)
    print "new times"
    print sorted(tsa2[tsa2.keys()[0]].get_times())
    assert tsa != tsa2
    assert all(key in tsa2.keys() for key in tsa.keys())
    assert len(tsa) == len(tsa2)
    return tsa2

def shootout(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.read_tsa_full(datestring, force=True)
    starttime = time.time()
    tsa.dump_to_csv(gzip.open("/tmp/test_tsa.csv.gz", "wb"))
    tsa2 = TimeseriesArray.load_from_csv(gzip.open("/tmp/test_tsa.csv.gz", "rb"))
    assert tsa == tsa2
    print "CSV Export/Import of whole tsa Duration %s" % (time.time() - starttime)
    starttime = time.time()
    cPickle.dump(tsa, gzip.open("/tmp/test_tsa_cPickle.gz", "wb"))
    tsa2 = cPickle.load(gzip.open("/tmp/test_tsa_cPickle.gz", "rb"))
    assert tsa == tsa2
    print "cPickle Export/Import of whole tsa Duration %s" % (time.time() - starttime)
    starttime = time.time()
    for key, ts in tsa.items():
        filehandle = gzip.open("/tmp/test_ts.csv.gz", "wb")
        ts.dump_to_csv(filehandle)
        filehandle.close()
        filehandle = gzip.open("/tmp/test_ts.csv.gz", "rb")
        ts2 = Timeseries.load_from_csv(filehandle)
        filehandle.close()
        #print ts2
        #print ts.ts_keyname, ts2.ts_keyname
        #print ts.headers, ts2.headers
        #print key, ts == ts2
        assert ts == ts2
    print "CSV Export/Import Duration %s" % (time.time() - starttime)
    starttime = time.time()
    for key, ts in tsa.items():
        filehandle = gzip.open("/tmp/test1.cPickle.gz", "wb")
        cPickle.dump(ts, filehandle)
        filehandle.close()
        filehandle = gzip.open("/tmp/test1.cPickle.gz", "rb")
        ts2 = cPickle.load(filehandle)
        filehandle.close()
        #print ts2
        #print ts.ts_keyname, ts2.ts_keyname
        #print ts.headers, ts2.headers
        #print key, ts == ts2
        assert ts == ts2
    print "cPickle Export/Import Duration %s" % (time.time() - starttime)
    #keys = tsa.keys()
    #for slottime in get_slot_timeline(datestring, 600):
    #    print slottime, slottime in tsa[keys[0]].get_times()
    #    #print tuple((tsa[key].get_single_value(slottime, 'hrStorageAllocationFailures') for key in keys))

    return

def timeit(func, arg=(), kwds={}):
    starttime = time.time()
    ret = func(*arg, **kwds)
    print "Duration of %s : %f" % (func.__name__, time.time() - starttime)
    return ret

def benchmark(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    starttime = time.time()
    #tsa = datalogger.read_tsa_full(datestring, force=True)
    tsa = timeit(datalogger.load_tsa, (datestring, ),  {"cleancache" : False})
    timeit(tsa.dump_split, ("/tmp/", ), {"overwrite":False})
    tsa2 = timeit(tsa.load_split, ("/tmp/", datalogger.index_keynames))
    assert tsa2 == tsa
    timeit(tsa.dump_to_csv, (gzip.open("/tmp/test_tsa.csv.gz", "wb"), ))
    tsa2 = timeit(TimeseriesArray.load_from_csv, (gzip.open("/tmp/test_tsa.csv.gz", "rb"), ))
    assert tsa == tsa2
    starttime = time.time()
    dumpduration = 0
    loadduration = 0
    for key, ts in tsa.items():
        dumpstart = time.time()
        filehandle = gzip.open("/tmp/test_ts.csv.gz", "wb")
        ts.dump_to_csv(filehandle)
        filehandle.close()
        dumpduration += time.time() - dumpstart
        loadstart = time.time()
        filehandle = gzip.open("/tmp/test_ts.csv.gz", "rb")
        ts2 = Timeseries.load_from_csv(filehandle)
        filehandle.close()
        loadduration += time.time() - loadstart
        try:
            assert ts == ts2
        except AssertionError as exc:
            print "Assertion Error on key %s" % str(key)
    print "cummulated dump duration : %f" % dumpduration
    print "cummulated load duration : %f" % loadduration
    print "CSV Export/Import single ts Duration %s" % (time.time() - starttime)

def benchmark_load():
    # get data, from datalogger, or dataloggerhelper
    starttime = time.time()
    #tsa = datalogger.read_tsa_full(datestring, force=False)
    #print "cPickle import from raw and export every timeseries in %s" % (time.time() - starttime)
    print "starting benchmark"
    starttime = time.time()
    tsa2 = TimeseriesArray.load_from_csv(gzip.open("/tmp/test_tsa.csv.gz", "rb"))

def main():
    benchmark(datalogger, datestring)
    #report_ram(datalogger, datestring)

if __name__ == "__main__":
    project = "haproxy"
    tablename = "haproxylog"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    datestring = "2015-09-01"
    main()
    cProfile.run("benchmark_load()")
