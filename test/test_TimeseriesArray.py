#!/usr/bin/python2
from __future__ import print_function
import unittest
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import gzip
import os
# own modules
import datalogger4 # for assertIsInstance
from datalogger4.Timeseries import Timeseries as Timeseries
from datalogger4.TimeseriesArray import TimeseriesArray as TimeseriesArray
from datalogger4.TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats

meta2 = {
    "blacklist": [],
    "delimiter": "\t",
    "headers": [
        "fcIfC3Discards",
        "fcIfC3InFrames",
        "fcIfC3InOctets",
        "fcIfC3OutFrames",
        "fcIfC3OutOctets",
        "hostname",
        "ifDescr",
        "index",
        "ts"
    ],
    "index_keynames": [
        "hostname",
        "ifDescr"
    ],
    "interval": 300,
    "ts_keyname": "ts",
    "value_keynames": {
        "fcIfC3Discards": "counter64",
        "fcIfC3InFrames": "counter64",
        "fcIfC3InOctets": "counter64",
        "fcIfC3OutFrames": "counter64",
        "fcIfC3OutOctets": "counter64",
        "index": "asis"
    }
}

meta = {
    "blacklist": [],
    "delimiter": "\t",
    "headers": [
        "ts",
        "hostname",
        "com_select",
        "uptime",
        "com_insert",
        "slow_queries",
        "bytes_sent",
        "com_update",
        "connections",
        "com_delete",
        "qcache_hits",
        "questions",
        "opened_tables",
        "aborted_connects",
        "bytes_received",
        "created_tmp_tables",
        "created_tmp_disk_tables",
        "aborted_clients"
    ],
    "index_keynames": [
        "hostname"
    ],
    "interval": 300,
    "ts_keyname": "ts",
    "value_keynames": {
        "com_select": "persecond",
        "uptime": "asis",
        "com_insert": "persecond",
        "slow_queries": "asis",
        "bytes_sent": "persecond",
        "com_update": "persecond",
        "connections": "persecond",
        "com_delete": "persecond",
        "qcache_hits": "persecond",
        "questions": "persecond",
        "opened_tables": "asis",
        "aborted_connects": "persecond",
        "bytes_received": "persecond",
        "created_tmp_tables": "persecond",
        "created_tmp_disk_tables": "persecond",
        "aborted_clients": "persecond",
    }
}


class Test(unittest.TestCase):


    def setUp(self):
        self.basedir = "/var/rrd"
        self.datestring = "2015-11-30"
        self.testfile = "testdata/fcIfC3AccountingTable"
        index_keys = (u"hostname", ) # the unicode is important
        self.app = TimeseriesArray.load("testdata/", meta["index_keynames"], datatypes=meta["value_keynames"])

    def test_str(self):
        print("testing __str__")
        print(self.app)

    def getitem_grouped__(self, key):
        pass

    def test_items(self):
        print("testing items")
        for key, value in self.app.items():
            self.assertIsInstance(key, tuple)
            self.assertIsInstance(value, datalogger4.Timeseries)

    def test_values(self):
        print("testing values")
        for value in self.app.values():
            self.assertIsInstance(value, datalogger4.Timeseries)

    def test_keys(self):
        print("testing keys")
        for key in self.app.keys():
            self.assertIsInstance(key, tuple)

    def test_getitem(self):
        print("testing __getitem__")
        key = ('nagios.tilak.cc',)
        ts = self.app[key]
        assert ts[0][1] == 13353334.0

    def test_get_index_dict(self):
        print("testing get_index_dict")
        key = ('nagios.tilak.cc',)
        index_dict = self.app.get_index_dict(key)
        assert index_dict["hostname"] == key[0]

    def test_index_keynames(self):
        print("testing index_keynames")
        assert self.app.index_keynames == ('hostname',)

    def test_value_keynames(self):
        print("testing value_keynames")
        assert self.app.value_keynames == ['com_select', 'uptime', 'com_insert', 'slow_queries', 'bytes_sent', 'com_update', 'connections', 'com_delete', 'qcache_hits', 'questions', 'opened_tables', 'aborted_connects', 'bytes_received', 'created_tmp_tables', 'created_tmp_disk_tables', 'aborted_clients']

    def test_ts_key(self):
        print("testing ts_key")
        assert self.app.ts_key == "ts"

    def test_stats(self):
        print("testing stats")
        stats = self.app.stats
        self.assertIsInstance(stats, datalogger4.TimeseriesArrayStats)

    def test_cache(self):
        print("testing cache")
        # this test app is initialized without caching
        assert self.app.cache == False

    def set_group_keyname(self, index_keyname, group_func):
        pass

    def test_to_float(self):
        print("testing to_float")
        test = self.app.to_float("3.14255469657")
        assert test == 3.14255469657
        test = self.app.to_float("3,14255469657")
        assert test == 3.14255469657

    def add(self, data, group_func=None):
        pass
    def group_add(self, data, group_func):
        pass
    def append(self, key, timeserie):
        pass
    def groupby(self, fieldnum, group_func, time_func="avg"):
        pass

    def test_convert(self):
        print("testing load, cache, convert, slice")
        tsa = TimeseriesArray.load("testdata/", meta["index_keynames"], datatypes=meta["value_keynames"])
        tsa.cache = True # thats crucial, otherwise every timeseries will alwys be read from disk
        tsa.convert(colname="uptime", datatype="persecond", newcolname="uptime_persecond")
        tsa.convert("uptime", "derive", "uptime_derive")
        tsa.convert("uptime", "percent", "uptime_percent")
        tsa.convert("com_select", "counter32", "com_select_counter32")
        tsa.convert("com_select", "counter64", "com_select_counter64")
        tsa.convert("com_select", "gauge32", "com_select_gauge32")
        tsa.convert("com_select", "counterreset", "com_select_counterreset")
        assert all(newcol in tsa.value_keynames for newcol in ("uptime_persecond", "uptime_derive", "uptime_percent", "com_select_counter32", "com_select_counter64", "com_select_gauge32", "com_select_counterreset"))
        tsa1 = tsa.slice(("uptime", "uptime_persecond", "com_select_gauge32"))
        print(tsa1[('nagios.tilak.cc',)])

    def test_add_calc_col_single(self):
        print("testing load, cache, add_calc_col_single, slice")
        tsa = TimeseriesArray.load("testdata/", meta["index_keynames"], datatypes=meta["value_keynames"])
        tsa.cache = True
        tsa.add_calc_col_single("bytes_received", "kbytes_received", lambda a: a / 8)
        tsa1 = tsa.slice(("bytes_received", "kbytes_received"))
        print(tsa1[('nagios.tilak.cc',)])

    def test_add_calc_col_full(self):
        print("testing load, add_calc_col_full, cache, slice")
        tsa = TimeseriesArray.load("testdata/", meta["index_keynames"], datatypes=meta["value_keynames"])
        tsa.cache = True
        tsa.add_calc_col_full("kbytes_total", lambda a: (a["bytes_received"] + a["bytes_sent"]) / 8)
        tsa1 = tsa.slice(("bytes_received", "bytes_sent", "kbytes_total"))
        print(tsa1[('nagios.tilak.cc',)])

    def test_remove_col(self):
        print("testing remove_col, load, cache")
        tsa = TimeseriesArray.load("testdata/", meta["index_keynames"], datatypes=meta["value_keynames"])
        tsa.cache = True
        tsa.remove_col("uptime")
        assert "uptime" not in tsa.value_keynames
        try:
            tsa.remove_col("unknown")
        except KeyError:
            # should raise this exception
            pass

    def test_slice(self):
        print("testing slice")
        tsa = self.app.slice(("uptime",))
        # there should only 2 columns left
        assert len(tsa[('nagios.tilak.cc',)][0]) == 2
        # check latets uptime value
        assert tsa[('nagios.tilak.cc',)][-1][1] == 13439433.0

    def test_export(self):
        print("testing export, add")
        tsa = TimeseriesArray(meta["index_keynames"], meta["value_keynames"], "ts", datatypes=meta["value_keynames"])
        for entry in self.app.export():
            tsa.add(entry)
        assert tsa == self.app

    def test_dump(self):
        print("testing dump, get_ts_dumpfilename, __eq__")
        testdir = "testdata/tsa_testdump"
        if not os.path.isdir(testdir):
            os.mkdir(testdir)
        tsa = TimeseriesArray.load("testdata/fcIfC3AccountingTable", meta2["index_keynames"], datatypes=meta2["value_keynames"], filterkeys=None, index_pattern=None, matchtype="and")
        tsa.dump(testdir, overwrite=True)
        tsa1 = TimeseriesArray.load(testdir, meta2["index_keynames"], datatypes=meta2["value_keynames"], filterkeys=None, index_pattern=None, matchtype="and")
        assert tsa == tsa1

    def test_load(self):
        print("testing load, get_ts_filename, filtermatch, get_dumpfilename")
        tsa = TimeseriesArray.load("testdata/fcIfC3AccountingTable", meta2["index_keynames"], datatypes=meta2["value_keynames"], filterkeys=None, index_pattern=None, matchtype="and")
        #for key in tsa.keys():
        #    print(key)
        # loading only Timeseries matching this index key
        filterkeys = {"hostname" : "fca-sr2-8gb-21", "ifDescr" : None}
        tsa = TimeseriesArray.load("testdata/fcIfC3AccountingTable", meta2["index_keynames"], datatypes=meta2["value_keynames"], filterkeys=filterkeys, index_pattern=None, matchtype="and")
        for key in tsa.keys():
            assert key[0] == "fca-sr2-8gb-21"
        # loading only Timeseries matching this index key
        filterkeys = {"hostname" : None, "ifDescr" : "port-channel 1"}
        tsa = TimeseriesArray.load("testdata/fcIfC3AccountingTable", meta2["index_keynames"], datatypes=meta2["value_keynames"], filterkeys=filterkeys, index_pattern=None, matchtype="and")
        for key in tsa.keys():
            assert key[1] == "port-channel 1"
        # loading only Timeseries matching this index key using or
        filterkeys = {"hostname" : "fca-sr2-8gb-21", "ifDescr" : "port-channel 1"}
        tsa = TimeseriesArray.load("testdata/fcIfC3AccountingTable", meta2["index_keynames"], datatypes=meta2["value_keynames"], filterkeys=filterkeys, index_pattern=None, matchtype="or")
        for key in tsa.keys():
            assert key[1] == "port-channel 1" or key[0] == "fca-sr2-8gb-21"
        # using regular expression to filter some index_keys
        tsa = TimeseriesArray.load("testdata/fcIfC3AccountingTable", meta2["index_keynames"], datatypes=meta2["value_keynames"], filterkeys=None, index_pattern="(.*)fca-(.*)", matchtype="and")
        for key in tsa.keys():
            assert "fca-" in str(key)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
