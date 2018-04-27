#!/usr/bin/python

import urllib
import urllib2
from collections import OrderedDict as OrderedDict
import json
import base64
import unittest
import logging
import os
import requests
import calendar
import datetime
# own modules
from datalogger import Timeseries as Timeseries
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import TimeseriesStats as TimeseriesStats
from datalogger import DataLoggerRest as DataLoggerRest


class Test(unittest.TestCase):

    def setUp(self):
        self.datalogger = DataLoggerRest("https://datalogger-api.tirol-kliniken.cc/rest/v2")

    def test_projects(self):
        data = self.datalogger.get_projects()
        #logging.error(data)
        self.assertTrue(isinstance(data, list))

    def test_stat_func_names(self):
        data = self.datalogger.get_stat_func_names()
        #logging.error(data)
        self.assertTrue(isinstance(data, list))
        self.assertTrue(u"max" in data)

    def test_last_businessday_datestring(self):
        data = self.datalogger.get_last_businessday_datestring()
        #logging.error(data)
        self.assertTrue(isinstance(data, basestring))

    def test_caches(self):
        datestring = "2016-01-01"
        for project in self.datalogger.get_projects():
            tablenames = self.datalogger.get_tablenames(project)
            #logging.error(tablenames)
            for tablename in tablenames:
                cache = self.datalogger.get_cache(project, tablename, datestring)
                index_keys = self.datalogger.get_ts_index_keys(project, tablename, datestring)
                #logging.error(index_keys.keys())

    def test_tablenames(self):
        for project in self.datalogger.get_projects():
            tablenames = self.datalogger.get_tablenames(project)
            #logging.error(tablenames)
            for tablename in tablenames:
                tabledata = self.datalogger.get_meta(project, tablename)
                #logging.error(tabledata)

    def test_ts(self):
        project = self.datalogger.get_projects()[0]
        tablename = self.datalogger.get_tablenames(project)[0]
        datestring = "2016-08-01"
        # find one index_key
        index_key = self.datalogger.get_ts_index_keys(project, tablename, datestring).keys()[0]
        # base64 encoded
        index_key_b64 = base64.b64encode(str(index_key))
        ts1 = self.datalogger.get_ts(project, tablename, datestring, eval(index_key))
        #logging.error(ts1)
        ts2 = self.datalogger.get_ts(project, tablename, datestring, index_key_b64)
        self.assertEqual(ts1, ts2)
        tsastat = self.datalogger.get_tsastat(project, tablename, datestring)
        #logging.error(tsastat)
        tsastat_max = self.datalogger.get_tsastat(project, tablename, datestring, "max")
        #logging.error(tsastat_max)
        quantile = self.datalogger.get_quantile(project, tablename, datestring)
        #logging.error(quantile)

    def test_monthly(self):
        project = self.datalogger.get_projects()[0]
        tablename = self.datalogger.get_tablenames(project)[0]
        datestring = "2016-08-01"
        index_key = self.datalogger.get_ts_index_keys(project, tablename, datestring).keys()[0]
        monthstring = "2016-08"
        stats = self.datalogger.get_monthstats(project, tablename, monthstring)
        for datestring, data in stats.items():
            self.assertTrue(isinstance(data, dict))
            self.assertTrue(isinstance(data.values()[0], dict))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
