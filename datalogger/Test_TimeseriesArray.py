#!/usr/bin/python2

import unittest
import logging
import datetime
import gzip
import os
# own modules
from TimeseriesArrayLazy import TimeseriesArray as TimeseriesArray


class Test(unittest.TestCase):


    def setUp(self):
        self.basedir = "/var/rrd"
        self.datestring = "2015-11-30"
        self.testfile = "/opt/datalogger/datalogger/testdata/"
        index_keys = ("hostname")
        self.app = TimeseriesArray.load(self.testfile, index_keys)
    
    def test_str(self):
        print(self.app)

    def getitem_grouped__(self, key):
        pass
    def items(self):
        pass
    def values(self):
        pass
    def keys(self):
        pass
    def get_index_dict(self, index_values):
        pass
    def index_keys(self):
        pass
    def index_keynames(self):
        pass
    def value_keys(self):
        pass
    def value_keynames(self):
        pass
    def ts_key(self):
        pass
    def stats(self):
        pass
    def debug(self):
        pass
    def debug(self, value):
        pass
    def cache(self):
        pass
    def cache(self, value):
        pass
    def set_group_keyname(self, index_keyname, group_func):
        pass
    def to_float(value_str):
        pass
    def add(self, data, group_func=None):
        pass
    def group_add(self, data, group_func):
        pass
    def append(self, key, timeserie):
        pass
    def groupby(self, fieldnum, group_func, time_func="avg"):
        pass
    def convert(self, colname, datatype, newcolname=None):
        pass
    def add_derive_col(self, colname, newcolname):
        pass
    def add_per_s_col(self, colname, newcolname):
        pass
    def add_calc_col_single(self, colname, newcolname, func=lambda a: a):
        pass
    def add_calc_col_full(self, newcolname, func):
        pass
    def remove_col(self, colname):
        pass
    def slice(self, colnames):
        pass
    def export(self):
        pass
    def dump(self, outpath, overwrite=False):
        pass
    def get_ts_dumpfilename(key):
        pass
    def get_dumpfilename(index_keys):
        pass
    def filtermatch(key_dict, filterkeys, matchtype):
        pass
    def get_ts_filenames(path, index_keys, filterkeys=None, matchtype="and"):
        pass
    def load(path, index_keys, filterkeys=None, index_pattern=None, matchtype="and", datatypes=None):
        pass

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
