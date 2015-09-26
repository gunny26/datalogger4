#!/usr/bin/python
import datetime
import time
import sys
import os
import cProfile
import cPickle
import collections
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import unittest
from Timeseries import Timeseries as Timeseries
from TimeseriesArray import TimeseriesArray as TimeseriesArray
from DataLogger import DataLogger as DataLogger
import DataLoggerHelper as dh

class Test(unittest.TestCase):
    """Unittest Classs for WsusDb"""

    basedir = "/var/rrd/"
    project = "test"
    tablename = "fcIfC3AccountingTable"
    datestring = "2015-05-23"
    ts_keyname= "ts"
    datalogger = DataLogger(basedir, project, tablename)
    start_ts, stop_ts = dh.get_ts_from_datestring(datestring)
    keys = ("hostname", "ifDescr")
    # dataset uncompressed
    result_23 = dh.read_day(datalogger, ts_keyname=ts_keyname, index_keynames=keys, datestring="2015-05-23")
    result_23a = datalogger.read_day(datestring="2015-05-23")
    # dataset compressed
    result_24 = dh.read_day(datalogger, ts_keyname=ts_keyname, index_keynames=keys, datestring="2015-05-24")
    result_24a = datalogger.read_day(datestring="2015-05-24")

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
        self.assertEqual(start_ts, time.timezone)
        self.assertEqual(stop_ts, 86399)

    def test_read_one(self):
        index_dict = collections.OrderedDict(sorted({"hostname":'fcb-sr1-8gb-12', "ifDescr":'fc1/5'}.items()))
        result = dh.read_one(self.datalogger, ts_keyname=self.ts_keyname, index_dict=index_dict, start_ts=self.start_ts, stop_ts=self.stop_ts)
        self.assertIs(type(result), TimeseriesArray)

    def test_read_day(self):
        keys = ("hostname", "ifDescr")
        self.assertIs(type(self.result_24), TimeseriesArray)

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
        self.assertIs(type(key), float)
        self.assertIs(type(value), dict)

    def test_dump_ts_dict(self):
        index_dict = collections.OrderedDict(sorted({"hostname":'fcb-sr1-8gb-12', "ifDescr":'fc1/5'}.items()))
        result1 = dh.read_one(self.datalogger, ts_keyname=self.ts_keyname, index_dict=index_dict, start_ts=self.start_ts, stop_ts=self.stop_ts)
        result2 = result1[('fcb-sr1-8gb-12', 'fc1/5')].dump_ts_dict(('fcIfC3OutFrames',))
        self.assertIs(type(result2), dict)
        key, value = result2.items()[0]
        self.assertIs(type(key), float)
        self.assertIs(type(value), tuple)

    def test_getitem(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result1 = self.result_23[("fca-sr2-07bc", "bay13")]
        self.assertIs(type(result1), Timeseries)

    def test_get_stats(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result1 = self.result_23[("fca-sr2-27bc", "bay2")].get_stats()
        should = {'std': 0.0, 'count': 288, 'min': 0.0, 'max': 0.0, 'sum': 0.0, 'median': 0.0, 'first': 0.0, 'geomean': 0.0, 'avg': 0.0, 'latest': 0.0}
        self.assertIs(type(result1), dict)
        self.assertTrue(result1['fcIfC3InOctets']["median"] == should["median"])

    def test_get_stat(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result1 = self.result_23[("fca-sr2-27bc", "bay2")].get_stat(func='std')
        self.assertIs(type(result1), dict)
        should = {'index': 0.0, 'fcIfC3InFrames': 0.0, 'fcIfC3OutOctets': 0.0, 'fcIfC3InOctets': 0.0, 'fcIfC3OutFrames': 0.0, 'fcIfC3Discards': 0.0}
        self.assertTrue(result1 == should)

    def test_add_derive_col(self):
        keys = ("hostname", "ifDescr")
        result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result.add_derive_col("fcIfC3InOctets", "fcIfC3InOctets_d")
        # the sum of the values of the derived series, has to be to
        # equal to last-first of the original series
        last = result[('fcb-sr1-8gb-12', 'fc1/5')].get_stat("last")
        first = result[('fcb-sr1-8gb-12', 'fc1/5')].get_stat("first")
        total_original = last["fcIfC3InOctets"] - first["fcIfC3InOctets"]
        total_derived = result[('fcb-sr1-8gb-12', 'fc1/5')].get_stat("sum")["fcIfC3InOctets_d"]
        #print total_original, total_derived
        self.assertTrue(total_original == total_derived)
        # so this is a test dataset, the result has to be fix
        self.assertEqual(total_original, 2422898425856.0)

    def test_per_s_col(self):
        keys = ("hostname", "ifDescr")
        result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        result.add_per_s_col("fcIfC3InOctets", "fcIfC3InOctets_s")
        print result.get_stats("fcIfC3InOctets_s")
        # the sum of the values of the derived series, has to be to
        # equal to last-first of the original series
        start_ts = result[('fcb-sr1-8gb-12', 'fc1/5')].get_start_ts()
        stop_ts = result[('fcb-sr1-8gb-12', 'fc1/5')].get_stop_ts()
        duration = stop_ts - start_ts
        last = result[('fcb-sr1-8gb-12', 'fc1/5')].get_stat("last")
        first = result[('fcb-sr1-8gb-12', 'fc1/5')].get_stat("first")
        total_original = last["fcIfC3InOctets"] - first["fcIfC3InOctets"]
        print "[('fcb-sr1-8gb-12', 'fc1/5')] fcIfC3InOctets : %s" % total_original
        total_per_s = result[('fcb-sr1-8gb-12', 'fc1/5')].get_stat("sum")["fcIfC3InOctets_s"] * duration
        print "[('fcb-sr1-8gb-12', 'fc1/5')] fcIfC3InOctets : %s" % total_per_s
        #print total_original, total_derived
        self.assertTrue(total_original == total_per_s)
        # so this is a test dataset, the result has to be fix
        self.assertEqual(total_original, 2422898425856.0)

    def test_slice(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        timeseries = self.result_23[('fcb-sr1-8gb-12', 'fc1/5')]
        subset = timeseries.slice(('fcIfC3OutOctets', 'fcIfC3InOctets'))
        self.assertTrue(('fcIfC3OutOctets', 'fcIfC3InOctets') == subset.get_headers())

    def test_serie(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        timeseries = self.result_23[('fcb-sr1-8gb-12', 'fc1/5')]
        serie = timeseries.get_serie('fcIfC3Discards')
        self.assertTrue(len(serie) == 288)
        self.assertTrue(type(serie) == tuple)
        self.assertTrue(serie[0] == 0.0)

    def test_times(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        timeseries = self.result_23[('fcb-sr1-8gb-12', 'fc1/5')]
        serie = timeseries.get_times()
        #print serie, type(serie), len(serie)
        self.assertTrue(len(serie) == 288)
        self.assertTrue(type(serie) == list)
        self.assertEqual(serie[0], 1432332032.0)
        self.assertEqual(serie[-1], 1432418176.0)

    def test_get_single_value(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        timeseries = self.result_23[('fcb-sr1-8gb-12', 'fc1/5')]
        #print "%f" % timeseries.get_single_value(1432386304.0, "fcIfC3OutOctets")
        self.assertTrue(timeseries.get_single_value(1432386304.0, "fcIfC3OutOctets") == 4245577319579648.000000)

    def no_test_save_csv(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        timeseries = self.result_23[('fcb-sr1-8gb-12', 'fc1/5')]
        headers = timeseries.get_headers()
        old_stats = timeseries.get_stats()
        timeseries.save_csv("/tmp/test.csv")
        timeseries.load_csv(headers, ts_keyname=self.ts_keyname, filename="/tmp/test.csv")
        new_stats = timeseries.get_stats()
        #print old_stats
        #print new_stats
        self.assertTrue(old_stats == new_stats)

    def no_test_save_csv(self):
        keys = ("hostname", "ifDescr")
        #result = dh.read_day(self.datalogger, ts_keyname=self.ts_keyname, index_keynames=keys, datestring="2015-05-23")
        dh.save_tsa(self.datalogger, self.result_23)
        print self.result_23.keys()
        timeseries = self.result_23[(u'fcb-sr1-8gb-12', u'fc1/5')]
        old_stats = timeseries.get_stats()
        loaded_tsa = dh.load_tsa(self.datalogger, self.ts_keyname, keys, 1432339206, 1432425382)
        loaded_timeseries = loaded_tsa[(u'fcb-sr1-8gb-12', u'fc1/5')]
        loaded_stats = loaded_timeseries.get_stats()
        #print old_stats
        #print loaded_stats
        self.assertTrue(old_stats == loaded_stats)

    def test_get_group_by_tsa(self):
        grouped_tsa = self.result_23.get_group_by_tsa(("hostname",), lambda a: sum(a))
        print grouped_tsa.keys()
        print grouped_tsa[('fcb-sr1-8gb-12', )].get_stats()
        self.assertEqual(grouped_tsa[('fcb-sr1-8gb-12', )].get_stats()["index"]['sum'], 329895641088.0)

    def test_interval(self):
        """
        tests TimeseriesArray method sanitize
        indirect test of Timeseries get_interval method also
        """
        self.result_23.sanitize()

    def test_get_scatter_data(self):
        tsa = self.result_23a.slice([u'fcIfC3InFrames', u'fcIfC3OutFrames'])
        tsa.add_per_s_col(u'fcIfC3InFrames', u'fcIfC3InFrames_s')
        tsa.add_per_s_col(u'fcIfC3OutFrames', u'fcIfC3OutFrames_s')
        tsa.remove_col(u'fcIfC3InFrames')
        tsa.remove_col(u'fcIfC3OutFrames')
        tsa_grouped = tsa.get_group_by_tsa(("hostname",), lambda a: sum(a))
        data = self.datalogger.get_scatter_data(tsa_grouped, u'fcIfC3InFrames_s', u'fcIfC3OutFrames_s', "mean")
        self.assertEqual(len(data), 22)
        self.assertEqual(data[0]["name"], "fca-sr1-19bc")
        self.assertEqual(data[0]["data"][0][0], 43726)
        self.assertEqual(data[0]["data"][0][1], 25608)


def test():
    logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    unittest.main()
    #main()
    #cProfile.run("main()")
