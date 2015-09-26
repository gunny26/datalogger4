#!/usr/bin/python
import datetime
import time
import sys
import os
import cProfile
import cPickle
import collections
import logging
logging.basicConfig(level=logging.DEBUG)
import unittest
import matplotlib.pyplot as plt
import tilak_wiki
from Timeseries import Timeseries as Timeseries
from TimeseriesArray import TimeseriesArray as TimeseriesArray
from DataLogger import DataLogger as DataLogger
import DataLoggerHelper as dh

def vm_datastore(datalogger, start_ts=None, stop_ts=None):
    ts_field = "ts" # column name of timestamp information
    stat_func = "avg" # how to agregate timeseries data over time
    delim = "\t" # delimiter used in raw data
    keys = ("hostname", "instance") # unique key fields of raw data
    #tsa = dh.read_python(datalogger, ts_keyname=ts_field, index_keynames=keys, start_ts=start_ts, stop_ts=stop_ts)
    tsa = dh.read_day(datalogger, ts_keyname=ts_field, index_keynames=keys, datestring="2015-05-26")
    print "length %d, size %d" % (len(tsa), sys.getsizeof(tsa))
    #print tsa.keys()
    print tsa.get_value_keys()
    data = tsa[('srvwebcf05.tilak.cc', '530c8ac2-7af8585b-62e0-0025b521006f')]
    test = cPickle.dumps(data)
    result1 = data.dump_list(('datastore.totalWriteLatency.average',))
    assert type(result1) == list
    print "first %d, last %d, length %d, size %d" % (result1[0][0], result1[-1][0], len(result1), sys.getsizeof(result1))
    result2 = data.dump_dict(('datastore.totalWriteLatency.average',))
    assert type(result2) == dict
    print "first %d, last %d, length %d, size %d" % (sorted(result2.keys())[0], sorted(result2.keys())[-1], len(result2), sys.getsizeof(result2))
    ts_dict = {}
    for index, row in enumerate(result1):
        if row[0] not in result2.keys():
            print row
        if row[0] not in ts_dict:
            ts_dict[row[0]] = True
        else:
            print "duplicate ts %s detected" % row[0]
    result3 = data.dump_ts_dict(('datastore.totalWriteLatency.average',))
    assert type(result3) == dict
    print "first %d, last %d, length %d, size %d" % (sorted(result3.keys())[0], sorted(result3.keys())[-1], len(result3), sys.getsizeof(result3))
    # test read_one
    starttime = time.time()
    print dh.read_one.__doc__
    index_dict = collections.OrderedDict(sorted({"hostname":'srvwebcf05.tilak.cc', "instance":'530c8ac2-7af8585b-62e0-0025b521006f'}.items()))
    print index_dict.keys()
    tsa = dh.read_one(datalogger, ts_keyname=ts_field, index_dict=index_dict, start_ts=start_ts, stop_ts=stop_ts)
    print tsa.keys()
    data = tsa[('srvwebcf05.tilak.cc', '530c8ac2-7af8585b-62e0-0025b521006f')]
    result1 = data.dump_list(('datastore.totalWriteLatency.average',))
    assert type(result1) == list
    print "first %d, last %d, length %d, size %d" % (result1[0][0], result1[-1][0], len(result1), sys.getsizeof(result1))
    logging.info("Duration %f s", time.time()-starttime)


def get_instances_for_server(keys, server):
    for servername, instance in sorted(keys):
        if server == servername:
            yield instance

def main1():
    basedir = "/var/rrd/"
    project = "vicenter"
    tablename = "virtualMachineDatastoreStats"
    start = datetime.datetime(2015, 5, 25, 0, 0, 0)
    start_ts = dh.datetime_to_ts(start)
    print start_ts, start
    stop = datetime.datetime(2015, 5, 25, 23, 59, 0)
    stop_ts = dh.datetime_to_ts(stop)
    print stop_ts, stop
    datalogger = DataLogger(basedir, project, tablename, delimiter="\t")
    ts_keyname = "ts"
    index_keynames = ("hostname", "instance")
    server = "srvpralki1.tilak.cc"
    datestring = "2015-05-31"
    tsa = dh.read_day(datalogger, ts_keyname=ts_keyname, index_keynames=index_keynames, datestring=datestring)
    for instance in get_instances_for_server(tsa.keys(), server):
        print "working on %s instance %s" % (server, instance)
        series = tsa[(server, instance)]
        print series.get_headers()
        for counter in series.get_headers():
            times = tuple((datetime.datetime.fromtimestamp(row[0]) for row in series.dump_list((counter,))))
            values = tuple((row[1] for row in series.dump_list((counter,))))
            fig, axis = plt.subplots()
            axis.plot(times, values, "r-")
            axis.set_ylabel(counter)
            axis.xaxis_date()
            #write_latencys = tuple((row[1] for row in series.dump_list(("datastore.totalWriteLatency.average",))))
            #print times
            #print values
            #plt.plot_date(x=times, y=values, fmt="r-", xdate=True)
            #write_f, write_ax, = plt.subplots()
            #write_ax.plot(times, read_latencys, "r-",)
            #write_ax.set_ylabel("datastore.totalWriteLatency.average")
            #write_ax.xaxis_date()
            #read_f, read_ax = plt.subplots()
            #read_ax.plot(times, read_latencys, "b-")
            #read_ax.set_ylabel("datastore.totalReadLatency.average")
            #read_ax.xaxis_date()
            #read_hist_f, read_hist_ax = plt.subplots()
            #read_hist_ax.hist(read_latencys)
    plt.grid(True)
    plt.show()
            #vm_datastore(datalogger, start_ts, stop_ts)

def get_column(data, colnum):
    for ts in data:
        for row in ts:
            yield row[colnum]

def main_group_by():
    basedir = "/var/rrd/"
    project = "test"
    tablename = "fcIfC3AccountingTable"
    datestring = "2015-05-23"
    ts_keyname= "ts"
    index_keynames = ("hostname", "ifDescr")
    datalogger = DataLogger(basedir, project, tablename, delimiter="\t")
    tsa = dh.read_day(datalogger, ts_keyname=ts_keyname, index_keynames=index_keynames, datestring=datestring)
    print tsa.keys()
    new_index_keys = ("hostname", )
    new_tsa = tsa.get_group_by_tsa(("hostname", ), group_func=lambda a: sum(a)/len(a))
    print new_tsa.keys()
    print new_tsa.get_value_keys()
    print new_tsa.get_stats("fcIfC3InOctets")

def main():
    basedir = "/var/rrd/"
    project = "test"
    tablename = "fcIfC3AccountingTable"
    datestring = "2015-05-23"
    ts_keyname= "ts"
    index_keynames = ("hostname", "ifDescr")
    datalogger = DataLogger(basedir, project, tablename, delimiter="\t")
    tsa = dh.read_day(datalogger, ts_keyname=ts_keyname, index_keynames=index_keynames, datestring=datestring)
    print tsa.keys()
    new_index_keys = ("hostname", )
    new_tsa = tsa.get_group_by_tsa(("hostname", ), group_func=lambda a: sum(a)/len(a))
    print new_tsa.keys()
    print new_tsa.get_value_keys()
    new_tsa.add_derive_col("fcIfC3InOctets", "fcIfC3InOctets_d")
    new_tsa.add_derive_col("fcIfC3OutOctets", "fcIfC3OutOctets_d")
    fcIn = dict(((key, stat_dict["median"]) for key, stat_dict in new_tsa.get_stats("fcIfC3InOctets_d").items()))
    fcOut = dict(((key, stat_dict["median"]) for key, stat_dict in new_tsa.get_stats("fcIfC3OutOctets_d").items()))
    hc_data = dict(((key[0], (fcIn[key], fcOut[key])) for key in fcIn.keys()))
    for key, value in hc_data.items():
        print "{name:'%s', data:[%s]}," % (key, list(value))


class Test(unittest.TestCase):
    """Unittest Classs for WsusDb"""

    basedir = "/var/rrd/"
    project = "test"
    tablename = "fcIfC3AccountingTable"
    datestring = "2015-05-23"
    ts_keyname= "ts"
    datalogger = DataLogger(basedir, project, tablename, delimiter="\t")
    start_ts, stop_ts = dh.get_ts_from_datestring(datestring)

    def no_test_public_methods(self):
        """
        Method to test if all public methods of testclass are tested
        """
        testclass = dh
        state = True
        for item in dir(testclass):
            method = getattr(testclass, item)
            if (callable(method)) and (not item.startswith("_")):
                test_method = "test_%s" % item
                if test_method not in dir(self):
                    logging.error("No Testmethod found for Method %s" % item)
                    state = False
        self.assertIs(state, True)

    def test_get_ts_from_datestring(self):
        datestring = "1970-01-01"
        start_ts, stop_ts = dh.get_ts_from_datestring(datestring)
        self.assertEqual(start_ts, 0)
        self.assertEqual(stop_ts, 86399)

    def test_read_one(self):
        index_dict = collections.OrderedDict(sorted({"hostname":'fcb-sr1-8gb-12', "ifDescr":'fc1/5'}.items()))
        result = dh.read_one(self.datalogger, ts_keyname=self.ts_keyname, index_dict=index_dict, start_ts=self.start_ts, stop_ts=self.stop_ts)
        self.assertIs(type(result), TimeseriesArray)

    def test_read_day(self):
        keys = ("hostname", "ifDescr")
        result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-24")
        self.assertIs(type(result), TimeseriesArray)

    def test_dump_list(self):
        index_dict = collections.OrderedDict(sorted({"hostname":'fcb-sr1-8gb-12', "ifDescr":'fc1/5'}.items()))
        result1 = dh.read_one(self.datalogger, ts_keyname=self.ts_keyname, index_dict=index_dict, start_ts=self.start_ts, stop_ts=self.stop_ts)
        result2 = result1[('fcb-sr1-8gb-12', 'fc1/5')].dump_list(('fcIfC3OutFrames',))
        self.assertIs(type(result2), list)
        self.assertIs(type(result2[0]), list)

    def test_dump_dict(self):
        index_dict = collections.OrderedDict(sorted({"hostname":'fcb-sr1-8gb-12', "ifDescr":'fc1/5'}.items()))
        result1 = dh.read_one(self.datalogger, ts_keyname=self.ts_keyname, index_dict=index_dict, start_ts=self.start_ts, stop_ts=self.stop_ts)
        result2 = result1[('fcb-sr1-8gb-12', 'fc1/5')].dump_dict(('fcIfC3OutFrames',))
        self.assertIs(type(result2), dict)
        key, value = result2.items()[0]
        self.assertIs(type(key), int)
        self.assertIs(type(value), dict)

    def test_dump_ts_dict(self):
        index_dict = collections.OrderedDict(sorted({"hostname":'fcb-sr1-8gb-12', "ifDescr":'fc1/5'}.items()))
        result1 = dh.read_one(self.datalogger, ts_keyname=self.ts_keyname, index_dict=index_dict, start_ts=self.start_ts, stop_ts=self.stop_ts)
        result2 = result1[('fcb-sr1-8gb-12', 'fc1/5')].dump_ts_dict(('fcIfC3OutFrames',))
        self.assertIs(type(result2), dict)
        key, value = result2.items()[0]
        self.assertIs(type(key), int)
        self.assertIs(type(value), tuple)

    def test_getitem(self):
        keys = ("hostname", "ifDescr")
        result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result1 = result[("fca-sr2-07bc", "bay13")]
        self.assertIs(type(result1), Timeseries)

    def test_get_stats(self):
        keys = ("hostname", "ifDescr")
        result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result1 = result[("fca-sr2-27bc", "bay2")].get_stats()
        should = {'std': 0.0, 'count': 288, 'min': 0.0, 'max': 0.0, 'sum': 0.0, 'median': 0.0, 'first': 0.0, 'geomean': 0.0, 'avg': 0.0, 'latest': 0.0}
        self.assertIs(type(result1), dict)
        self.assertTrue(result1['fcIfC3InOctets'] == should)

    def test_get_stat(self):
        keys = ("hostname", "ifDescr")
        result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result1 = result[("fca-sr2-27bc", "bay2")].get_stat(func='std')
        self.assertIs(type(result1), dict)
        should = {'index': 0.0, 'fcIfC3InFrames': 0.0, 'fcIfC3OutOctets': 0.0, 'fcIfC3InOctets': 0.0, 'fcIfC3OutFrames': 0.0, 'fcIfC3Discards': 0.0}
        self.assertTrue(result1 == should)

    def test_add_derive_col(self):
        keys = ("hostname", "ifDescr")
        result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result.add_derive_col("fcIfC3InOctets", "fcIfC3InOctets_d")


def test():
    logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    #unittest.main()
    main()
    #cProfile.run("main()")
