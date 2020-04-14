#!/usr/bin/python3
from __future__ import print_function
import unittest
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import gzip
import json
import os
# own modules
import datalogger4 # to use assertIsInstance for testing
from datalogger4.DataLogger import DataLogger
from datalogger4.Timeseries import Timeseries
from datalogger4.TimeseriesArray import TimeseriesArray
from datalogger4.TimeseriesStats import TimeseriesStats
from datalogger4.TimeseriesArrayStats import TimeseriesArrayStats
from datalogger4.Quantile import QuantileArray
from datalogger4.Quantile import Quantile

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
        print("testing __getitem__")
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        caches = dl["caches"]
        print(caches)
        assert isinstance(caches, dict)
        tsa = dl["tsa"]
        print(tsa, type(tsa))
        self.assertIsInstance(tsa, datalogger4.TimeseriesArray)
        ts = dl["tsa", ("nagios.tilak.cc",)]
        print(ts)
        self.assertIsInstance(ts, datalogger4.Timeseries)
        assert tsa[("nagios.tilak.cc",)] == ts
        tsastats = dl["tsastats"]
        print(tsastats)
        self.assertIsInstance(tsastats, datalogger4.TimeseriesArrayStats)
        tsstats = dl["tsastats", ("nagios.tilak.cc",)]
        print(tsstats)
        self.assertIsInstance(tsstats, datalogger4.TimeseriesStats)
        assert tsastats[("nagios.tilak.cc",)] == tsstats
        qa = dl["qa"]
        print(qa)
        self.assertIsInstance(qa, datalogger4.QuantileArray)
        quantile = dl["qa", ("nagios.tilak.cc",)]
        print(quantile)
        assert isinstance(quantile, dict)
        assert qa[("nagios.tilak.cc",)] == quantile

    def test_load_tsa(self):
        print("testing delete_caches, load_tsa")
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
        print("testing delete_caches, load_tsastats")
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
        print("testing delete_caches, load_quantile")
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
        print("testing delete_caches, get_caches")
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        print(dl.get_caches())
        tsa = dl.load_tsa()
        print(dl.get_caches())

    def test_total_stats(self):
        print("testing delete_caches, load_total_stats")
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        total_stats = dl.load_total_stats()
        print(json.dumps(total_stats, indent=4))

    def test_raw_reader(self):
        print("testing delete_caches, raw_reader")
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        for row in dl.raw_reader():
            pass
        assert row['bytes_received'] == '272517939'

    def test_generate_caches(self):
        print("testing generate_caches")
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        cache = dl["caches"]
        assert not cache["ts"]["keys"]
        dl.generate_caches()
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        cache = dl["caches"]
        assert cache["ts"]["keys"]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
