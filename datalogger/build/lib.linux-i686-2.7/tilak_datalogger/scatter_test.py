#!/usr/bin/python
import datetime
import time
import sys
import os
import cProfile
import cPickle
import collections
import json
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

    def test_get_ts_from_datestring(self):
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
