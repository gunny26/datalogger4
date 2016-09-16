#!/usr/bin/python

import unittest
import logging
import datetime
import os
# own modules
from datalogger import Timeseries as Timeseries
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import TimeseriesStats as TimeseriesStats
from datalogger import QuantileArray as QuantileArray
from datalogger import DataLogger as DataLogger


class Test(unittest.TestCase):


    def setUp(self):
        self.basedir = "/var/rrd"
        self.datestring = "2015-11-30"
        self.project = DataLogger.get_projects(self.basedir)[0]
        self.tablename = DataLogger.get_tablenames(self.basedir, self.project)[0]
        self.datalogger = DataLogger(self.basedir, self.project, self.tablename)

    def test_simple(self):
        self.assertTrue(self.datalogger.project == self.project)
        self.assertTrue(self.datalogger.tablename == self.tablename)
        self.assertTrue(isinstance(self.datalogger.delimiter, basestring))
        self.assertTrue(isinstance(self.datalogger.ts_keyname, basestring))
        self.assertTrue(isinstance(self.datalogger.headers, tuple))
        self.assertTrue(isinstance(self.datalogger.value_keynames, tuple))
        self.assertTrue(all((keyname in self.datalogger.headers for keyname in self.datalogger.value_keynames)))
        self.assertTrue(isinstance(self.datalogger.index_keynames, tuple))
        self.assertTrue(all((keyname in self.datalogger.headers for keyname in self.datalogger.index_keynames)))
        self.assertTrue(isinstance(self.datalogger.blacklist, tuple))
        self.assertTrue(all((keyname in self.datalogger.headers for keyname in self.datalogger.blacklist)))
        self.assertTrue(isinstance(self.datalogger.raw_basedir, basestring))
        self.assertTrue(os.path.exists(self.datalogger.raw_basedir))
        self.assertTrue(os.path.isdir(self.datalogger.raw_basedir))
        self.assertTrue(isinstance(self.datalogger.global_cachedir, basestring))
        self.assertTrue(os.path.exists(self.datalogger.global_cachedir))
        self.assertTrue(os.path.isdir(self.datalogger.global_cachedir))
        # meta is something like this
        # {u'ts_keyname': u'ts',
        # 'stat_func_names': [u'count', ... ],
        # u'interval': 300,
        # u'blacklist': [],
        # u'headers': [u'ts', u'http_host', ... ],
        # u'delimiter': u'\t',
        # u'value_keynames': {
        #   u'actconn': u'asis',
        #   u'hits': u'asis',
        #   ...
        #   },
        # u'index_keynames': [u'http_host']}
        self.assertTrue(self.datalogger.meta["headers"] == list(self.datalogger.headers))
        self.assertTrue(self.datalogger.meta["value_keynames"].keys() == list(self.datalogger.value_keynames))
        self.assertTrue(self.datalogger.meta["index_keynames"] == list(self.datalogger.index_keynames))
        self.assertTrue(self.datalogger.meta["blacklist"] == list(self.datalogger.blacklist))
        self.assertTrue(self.datalogger.meta["delimiter"] == self.datalogger.delimiter)
        self.assertTrue(self.datalogger.meta["ts_keyname"] == self.datalogger.ts_keyname)
        self.assertTrue(isinstance(self.datalogger.meta["stat_func_names"], list))

    def test_statics(self):
        self.assertTrue(isinstance(DataLogger.get_user(self.basedir), basestring))
        self.assertTrue(isinstance(DataLogger.get_group(self.basedir), basestring))
        self.assertTrue(isinstance(DataLogger.get_yesterday_datestring(), basestring))
        lbd = DataLogger.get_last_business_day_datestring()
        self.assertTrue(isinstance(DataLogger.get_last_business_day_datestring(), basestring))
        self.assertTrue(isinstance(DataLogger.datestring_to_date(lbd), datetime.date))
        for datestring in DataLogger.datewalker("2016-01-01", "2016-02-29"):
            self.assertTrue(isinstance(datestring, basestring))
        for datestring in DataLogger.monthwalker("2016-02"):
            self.assertTrue(isinstance(datestring, basestring))
        self.assertEqual(list(DataLogger.monthwalker("2016-02"))[-1], "2016-02-29")
        self.assertTrue(isinstance(DataLogger.get_ts_for_datestring("2016-01-01"), tuple))
        self.assertTrue(isinstance(DataLogger.get_ts_for_datestring("2016-01-01")[0], float))
        self.assertTrue(isinstance(DataLogger.get_ts_for_datestring("2016-01-01")[1], float))


    def test_data(self):
        self.datalogger.load_tsa(self.datestring)
        self.datalogger.load_tsastats(self.datestring)
        self.datalogger.load_correlationmatrix(self.datestring)
        self.datalogger.load_quantile(self.datestring)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
