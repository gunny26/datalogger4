#!/usr/bin/python2
from __future__ import print_function
import unittest
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import gzip
import json
import os
# own modules
import datalogger3
from Timeseries import Timeseries as Timeseries
from TimeseriesStats import TimeseriesStats as TimeseriesStats
from TimeseriesArray import TimeseriesArray as TimeseriesArray
from TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats

def calllogger(func):
    def wrapper(*args, **kwds):
        print("%s(args=%s, kwds=%s)" % (func.__name__, args, kwds))
        result = func(*args, **kwds)
        return result
    return wrapper

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
        tsa = TimeseriesArray.load("testdata/", meta["index_keynames"], datatypes=meta["value_keynames"])
        self.tsastats = TimeseriesArrayStats(tsa)

    def test__str__(self):
        print("testing __str__")
        print(self.tsastats)

    def __eq__(self, other):
        pass
    def __len__(self):
        pass
    def __getitem__(self, key):
        pass
    def __delitem__(self, key):
        pass

    def test_keys(self):
        print("testing keys")
        print(list(self.tsastats.keys()))
        assert all([key in [('srvwebsql2.tilak.cc',), ('srvdmzsql1.tilak.cc',), ('srvaporti1.tilak.cc',), ('nagios.tilak.cc',), ('srvazwsql1.tilak.cc',)] for key in self.tsastats.keys()])

    def test_values(self):
        assert all((isinstance(value, datalogger3.TimeseriesStats) for value in self.tsastats.values()))

    def test_items(self):
        for key, value in self.tsastats.items():
            assert isinstance(key, tuple)
            assert isinstance(value, datalogger3.TimeseriesStats)

    def test_index_keynames(self):
        assert self.tsastats.index_keynames == ('hostname',)

    def test_value_keynames(self):
        assert self.tsastats.value_keynames == ('com_select', 'uptime', 'com_insert', 'slow_queries', 'bytes_sent', 'com_update', 'connections', 'com_delete', 'qcache_hits', 'questions', 'opened_tables', 'aborted_connects', 'bytes_received', 'created_tmp_tables', 'created_tmp_disk_tables', 'aborted_clients')

    def test_slice(self):
        tsastats = self.tsastats.slice(("bytes_sent", "bytes_received"))
        assert tsastats.value_keynames == ("bytes_sent", "bytes_received")
        print(tsastats)

    def test_get_stats(self):
        stats = self.tsastats.get_stats("bytes_sent")
        assert stats[('nagios.tilak.cc',)]['max'] == 42969066.8

    def test_dump(self):
        outdir = "testdata/tsastat_testdump"
        if not os.path.isdir(outdir):
            os.mkdir(outdir)
        self.tsastats.dump(outdir, overwrite=True)
        tsastats = TimeseriesArrayStats.load(outdir, meta["index_keynames"], filterkeys=None, matchtype="and")
        assert tsastats == self.tsastats

    def filtermatch(key_dict, filterkeys, matchtype):
        pass

    def get_load_filenames(path, index_keys, filterkeys=None, matchtype="and"):
        pass

    def test_load(self):
        tsastat = TimeseriesArrayStats.load("testdata/", meta["index_keynames"], filterkeys=None, matchtype="and")
        assert self.tsastats == tsastat

    def test_from_json(self):
        tsastats = TimeseriesArrayStats.from_json(self.tsastats.to_json())
        assert tsastats == self.tsastats

    def remove_by_value(self, value_key, stat_func_name, value):
        pass

    def test_to_csv(self):
        for row in self.tsastats.to_csv("avg", sortkey=None, reverse=True):
            print(row)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
