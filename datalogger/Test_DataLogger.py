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
from DataLogger import DataLogger as DataLogger
from Timeseries import Timeseries as Timeseries
from TimeseriesArray import TimeseriesArray as TimeseriesArray
from TimeseriesStats import TimeseriesStats as TimeseriesStats
from TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats
from Quantile import QuantileArray as QuantileArray
from Quantile import Quantile as Quantile

class Test(unittest.TestCase):

    def setUp(self):
        self.basedir = "testdata"
        self.project = "mysql"
        self.tablename = "performance"
        self.datestring = "2018-04-01"
        self.datalogger = DataLogger(self.basedir)

    def notest__str__(self):
        print(self.datalogger)

    def test__init__(self):
        try:
            DataLogger("/nonexisting")
        except AttributeError as exc:
            print("Expected Exception: %s" % exc)
        try:
            dl = DataLogger("testdata")
            dl.setup("unknownproject", self.tablename, "2018-04-01")
        except AttributeError as exc:
            print("Expected Exception: %s" % exc)
        try:
            DataLogger("testdata")
            dl.setup("sanportperf", "unknowntablename", "2018-04-01")
        except AttributeError as exc:
            print("Expected Exception: %s" % exc)
        try:
            DataLogger("testdata")
            dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        except AttributeError as exc:
            print("Expected Exception: %s" % exc)

    def test__getitem__(self):
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        caches = dl["caches"]
        print(caches)
        assert isinstance(caches, dict)
        tsa = dl["tsa"]
        print(tsa)
        assert isinstance(tsa, TimeseriesArray)
        ts = dl["tsa", ("nagios.tilak.cc",)]
        print(ts)
        assert isinstance(ts, Timeseries)
        assert tsa[("nagios.tilak.cc",)] == ts
        tsastats = dl["tsastats"]
        print(tsastats)
        assert isinstance(tsastats, TimeseriesArrayStats)
        tsstats = dl["tsastats", ("nagios.tilak.cc",)]
        print(tsstats)
        assert isinstance(tsstats, TimeseriesStats)
        assert tsastats[("nagios.tilak.cc",)] == tsstats
        qa = dl["qa"]
        print(qa)
        assert isinstance(qa, QuantileArray)
        quantile = dl["qa", ("nagios.tilak.cc",)]
        print(quantile)
        assert isinstance(quantile, dict)
        assert qa[("nagios.tilak.cc",)] == quantile

    def test_load_tsa(self):
        dl = DataLogger("testdata")
        dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        dl.delete_caches()
        tsa = dl.load_tsa()
        #print(tsa)
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        tsa = dl.load_tsa()
        #print(tsa)

    def test_load_tsastats(self):
        dl = DataLogger("testdata")
        dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        dl.delete_caches()
        tsastats = dl.load_tsastats()
        #print(tsa)
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        tsastats = dl.load_tsastats()
        #print(tsa)

    def test_load_quantiles(self):
        dl = DataLogger("testdata")
        dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        dl.delete_caches()
        quantiles = dl.load_quantile()
        #print(tsa)
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        quantiles = dl.load_quantile()
        #print(tsa)

    def test_load_caches(self):
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        print(dl.get_caches())
        tsa = dl.load_tsa()
        print(dl.get_caches())

    def test_raw_reader(self):
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        for row in dl.raw_reader():
            pass
        assert row['bytes_received'] == '272517939'



if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
