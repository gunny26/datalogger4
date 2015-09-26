#!/usr/bin/python
import datetime
import time
import sys
import os
import cProfile
import cPickle
import collections
import json
import base64
import gzip
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
    result_23a = datalogger.read_day(datestring="2015-05-23")

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

    def test_get_keys(self):
        #print self.datalogger.get_index_keynames()
        datestring = "2015-05-23"
        keys = self.datalogger.filter_keys({u"hostname" : u"fca-sr1-11bc"})
        tsa = self.datalogger.load_tsa_filtered(datestring, {u"hostname" : u"fca-sr1-11bc"})
        self.assertEqual(len(keys), len(tsa))
        keys = self.datalogger.filter_keys({u'ifDescr': u'fc1/3'})
        tsa = self.datalogger.load_tsa_filtered(datestring, {u'ifDescr': u'fc1/3'})
        self.assertEqual(len(keys), len(tsa))
        keys = self.datalogger.load_tsa_filtered(datestring, {u'ifDescr': u'fc1/3', u"hostname" : u"fca-sr1-11bc"})
        tsa = self.datalogger.load_tsa_filtered(datestring, {u'ifDescr': u'fc1/3', u"hostname" : u"fca-sr1-11bc"})
        self.assertEqual(len(keys), len(tsa))

    def test_get_ts_from_datestring(self):
        datestring = "2015-05-23"
        tsa = TimeseriesArray(self.datalogger.get_index_keynames(), list(self.datalogger.get_value_keynames()), self.datalogger.get_ts_keyname())
        table_cachedir = os.path.join(self.datalogger.global_cachedir, datestring, self.datalogger.get_project(), self.datalogger.get_tablename())
        for filename in os.listdir(table_cachedir):
            full_filename = os.path.join(table_cachedir, filename)
            key_enc = filename.split(".")[0]
            key = eval(base64.b64decode(key_enc))
            logging.info("reading key %s from %s", key, filename)
            timeseries = self.datalogger.read_cachefile_single_timeseries(datestring, key)
            timeseries2 = cPickle.load(gzip.open(full_filename, "rb"))
            self.assertEqual(len(timeseries), len(timeseries2))
            tsa.append(key, timeseries)
        self.assertEqual(len(self.result_23a), len(tsa))
        self.assertEqual(self.result_23a.get_stats(u"fcIfC3Discards"), tsa.get_stats(u"fcIfC3Discards"))

def test():
    logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    unittest.main()
    #main()
    #cProfile.run("main()")
