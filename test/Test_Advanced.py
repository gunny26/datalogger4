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
import datalogger4 # for assertIsInstance Testing
import datalogger4.Advanced as Advanced
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

    def test_tsa_group_by(self):
        dl = DataLogger("testdata")
        dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        tsa = dl["tsa"]
        assert len(tsa) == 712
        print(tsa[('fcb-sr3-4gb-32', 'port-channel 2')])
        tsa_grouped = Advanced.tsa_group_by(tsa, dl.datestring, index_keynames=("hostname",), group_func=lambda a, b : (a + b) / 2, interval=dl.interval)
        assert len(tsa_grouped) == 24
        print(tsa_grouped[('fcb-sr3-4gb-32',)])
        tsa_total = Advanced.tsa_group_by(tsa, dl.datestring, index_keynames=(), group_func=lambda a, b : (a + b) / 2, interval=dl.interval)
        assert len(tsa_total) == 1
        print(tsa_total[()])

    def test_tsastats_group_by(self):
        dl = DataLogger("testdata")
        dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        tsastats = dl["tsastats"]
        assert len(tsastats) == 712
        tsastats_grouped = Advanced.tsastats_group_by(tsastats, index_keynames=("hostname",))
        assert len(tsastats_grouped) == 24
        self.assertIsInstance(tsastats_grouped, datalogger4.TimeseriesArrayStats)
        self.assertIsInstance(tsastats_grouped[('fcb-sr3-4gb-32',)], datalogger4.TimeseriesStats)
        print(tsastats_grouped[('fcb-sr3-4gb-32',)])
        tsastats_total = Advanced.tsastats_group_by(tsastats, index_keynames=())
        assert len(tsastats_total) == 1
        self.assertIsInstance(tsastats_total, datalogger4.TimeseriesArrayStats)
        self.assertIsInstance(tsastats_total[('__total__',)], datalogger4.TimeseriesStats)
        print(tsastats_total[('__total__',)])

    def test_get_scatterdata(self):
        dl = DataLogger("testdata")
        dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        tsastats = dl["tsastats"]
        tsastats_grouped = Advanced.tsastats_group_by(tsastats, index_keynames=("hostname",))
        scatter = Advanced.get_scatter_data(tsastats_grouped, ("fcIfC3InOctets", "fcIfC3OutOctets"), "avg")
        print(json.dumps(scatter, indent=4))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
