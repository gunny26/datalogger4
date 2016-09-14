#!/usr/bin/python

import unittest
import logging
# own modules
from datalogger import Timeseries as Timeseries
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import TimeseriesStats as TimeseriesStats
from datalogger import QuantileArray as QuantileArray
from datalogger import DataLogger as DataLogger


class Test(unittest.TestCase):


    def setUp(self):
        basedir = "/var/rrd"
        self.datestring = "2015-11-30"
        self.project = DataLogger.get_projects(basedir)[0]
        self.tablename = DataLogger.get_tablenames(basedir, self.project)[0]
        self.datalogger = DataLogger(basedir, self.project, self.tablename)

    def test_get_headers(self):
        data = self.datalogger.get_headers(self.project, self.tablename)
        assert isinstance(data, list)

    def test_get_index_keynames(self):
        data = self.datalogger.get_index_keynames(self.project, self.tablename)
        assert isinstance(data, list)

    def test_get_value_keynames(self):
        data = self.datalogger.get_value_keynames(self.project, self.tablename)
        assert isinstance(data, list)

    def test_get_ts_keyname(self):
        data = self.datalogger.get_ts_keyname(self.project, self.tablename)
        assert isinstance(data, basestring)

    def test_get_last_business_day_datestring(self):
        data = self.datalogger.get_last_business_day_datestring()
        assert isinstance(data, basestring)

    def test_get_datewalk(self):
        data = self.datalogger.datewalk("2015-11-01", "2015-11-30")
        self.assertGreater(len(data), 1)

    def test_get_caches(self):
        caches = self.datalogger.get_caches(self.project, self.tablename, self.datestring)
        self.assertTrue(isinstance(caches, dict))
        self.assertTrue(all((key in caches.keys() for key in [u'tsa', u'tsstat', u'tsastat', u'ts', u"quantile"])))
        for key, filename in caches["ts"]["keys"].items()[:10]:
            tsa = self.datalogger.get_ts(self.project, self.tablename, self.datestring, key)
            assert isinstance(tsa, TimeseriesArray)
            ts = tsa[tsa.keys()[0]]
            assert isinstance(ts, Timeseries)
            assert len(ts) > 0

    def test_get_tsa(self):
        tsa = self.datalogger.get_tsa(self.project, self.tablename, self.datestring)
        assert isinstance(tsa, TimeseriesArray)

    def test_get_tsa_adv(self):
        tsa = self.datalogger.get_tsa_adv(self.project, self.tablename, self.datestring, None, "avg", "(.*)")
        assert isinstance(tsa, TimeseriesArray)
        assert len(tsa.keys()) > 0

    def test_get_ts(self):
        tsa = self.datalogger.get_tsa(self.project, self.tablename, self.datestring)
        self.assertTrue(isinstance(tsa, TimeseriesArray))
        self.assertGreater(len(tsa.keys()), 0)
        ts = self.datalogger.get_ts(self.project, self.tablename, self.datestring, tsa.keys()[0])
        self.assertTrue(isinstance(ts, TimeseriesArray))
        self.assertGreater(len(ts.keys()), 0)

    def test_get_tsastats(self):
        tsastats = self.datalogger.get_tsastats(self.project, self.tablename, self.datestring)
        assert isinstance(tsastats, TimeseriesArrayStats)
        assert len(tsastats.keys()) > 0
        tsstat = tsastats[tsastats.keys()[0]]
        assert isinstance(tsstat, TimeseriesStats)

    def test_get_stat_func_names(self):
        data = self.datalogger.get_stat_func_names()
        assert isinstance(data, list)
        assert len(data) > 1
        assert all((key in data for key in [u'count', u'max', u'diff', u'avg', u'inc', u'std', u'dec', u'last', u'min', u'sum', u'median', u'first', u'mean']))

    def test_get_quantile(self):
        qa = self.datalogger.get_quantile(self.project, self.tablename, self.datestring)
        assert isinstance(qa, QuantileArray)
        # TODO: do more checking

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
