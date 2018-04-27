#!/usr/bin/python3

import unittest
import logging
import datetime
import gzip
import os
import json
# own modules
from Timeseries import Timeseries as Timeseries


class Test(unittest.TestCase):


    def setUp(self):
        self.basedir = "/var/rrd"
        self.datestring = "2015-11-30"
        self.testfile = "testdata/ts_KHUnc3J2d2Vic3FsMi50aWxhay5jYycsKQ==.csv.gz"
        with gzip.open(self.testfile, "rt") as infile:
            self.app = Timeseries.load(infile)

    def test_ts_keyname(self):
        assert self.app.ts_keyname == "ts"

    def test_headers(self):
        assert self.app.headers == [
            'com_select', 'uptime', 'com_insert', 'slow_queries', 'bytes_sent',
            'com_update', 'connections', 'com_delete', 'qcache_hits', 'questions',
            'opened_tables', 'aborted_connects', 'bytes_received', 'created_tmp_tables',
            'created_tmp_disk_tables', 'aborted_clients'
        ]

    def test_colnames(self):
        assert self.app.colnames == (
            'ts', 'com_select', 'uptime', 'com_insert', 'slow_queries', 'bytes_sent',
            'com_update', 'connections', 'com_delete', 'qcache_hits', 'questions',
            'opened_tables', 'aborted_connects', 'bytes_received', 'created_tmp_tables',
            'created_tmp_disk_tables', 'aborted_clients'
        )

    def test_start_ts(self):
        assert self.app.start_ts == 1521500402.0
        assert isinstance(self.app.start_ts, float)

    def test_stop_ts(self):
        assert self.app.stop_ts == 1521586501.0
        assert isinstance(self.app.stop_ts, float)

    def test_stats(self):
        # print(self.app.stats)
        pass

    def test_interval(self):
        assert self.app.interval == 293.49193762983396
        assert isinstance(self.app.interval, float)

    def test_head(self):
        print(self.app.head(delimiter=";", nrows=2, headers=True))

    def test_tail(self):
        print(self.app.tail(delimiter=";", nrows=2, headers=True))

    def test_getitem(self):
        print("testing __getitem__")
        assert self.app[0] == [1521500402.0, 89169365.0, 926326.0, 1551896.0, 80.0, 457679815481.0, 14380469.0, 2495151.0, 887492.0, 92828248.0, 398684283.0, 3581.0, 6284.0, 58785646985.0, 56614358.0, 9134320.0, 3643.0]
        assert self.app[0][1] == 89169365.0
        assert self.app[(0, 1)] == 89169365.0
        assert self.app[1521500402.0] == [1521500402.0, 89169365.0, 926326.0, 1551896.0, 80.0, 457679815481.0, 14380469.0, 2495151.0, 887492.0, 92828248.0, 398684283.0, 3581.0, 6284.0, 58785646985.0, 56614358.0, 9134320.0, 3643.0]
        assert self.app["ts"][-1] == 1521586501.0
        assert self.app["ts"][0] == self.app.start_ts
        assert self.app[(10, "com_select")] == 89359228.0 == self.app[(10, 1)] == self.app[10][1]
        assert self.app[(1521586501.0, 1)] == 99254355.0 == self.app[(-1, 1)] == self.app[-1][1]

    def add(self, timestamp, values, suppress_non_steady_ts=True):
        # TODO: implement this
        pass

    def test_group_add(self):
        print("testing group_add")
        with gzip.open(self.testfile, "rt") as infile:
            ts = Timeseries.load(infile)
        for row in self.app:
            ts.group_add(row[0], row[1:], lambda a, b : a + b)
        ts1 = ts.slice(("bytes_received",))
        ts1.append("bytes_received_org", self.app.get_serie("bytes_received"))
        assert ts1[0][1] == ts1[0][2] + ts1[0][2]
        print(ts1)

    def test_resample(self):
        print("testing slice, resample")
        with gzip.open(self.testfile, "rt") as infile:
            ts = Timeseries.load(infile)
        ts1 = ts.slice(("com_select",))
        # TODO some check to be above actual interval
        ts2 = ts1.resample(3600, lambda a: sum(a))
        print(ts2)

    def test_to_csv(self):
        print("testing to_csv")
        assert len(list(self.app.to_csv(value_keynames=("uptime", "com_select")))) == 289

    def test_to_data(self):
        print("testing to_data")
        data = self.app.to_data()
        data_json = json.dumps(list(data))
        assert isinstance(data_json, str)

    def test_dump(self):
        print("testing dump, load")
        with gzip.open("testdata/ts_test.csv.gz", "wt") as outfile:
            self.app.dump(outfile)
        with gzip.open("testdata/ts_test.csv.gz", "rt") as infile:
            ts = Timeseries.load(infile)
        assert ts[0][0] == self.app[0][0]
        assert ts[-1][-1] == self.app[-1][-1]

    def test_get_serie(self):
        print("testing get_serie")
        # TODO: useless, use getitem
        assert self.app.get_serie("com_select") == self.app["com_select"]
        assert self.app.get_serie("com_select")[0] == 89169365.0
        assert self.app.get_serie("com_select")[-1] == 99254355.0

    def test_slice(self):
        print("testing slice")
        # TODO: only output function
        print(self.app.slice(('com_select', 'uptime')))

    def test_datatypes(self):
        self.app.datatypes == ['counter64', 'percent', 'persecond', 'counter32', 'derive', 'gauge32', 'counterreset']

    def test_convert(self):
        print("testing load, slice, convert")
        with gzip.open(self.testfile, "rt") as infile:
            ts = Timeseries.load(infile)
        ts1 = ts.slice(("uptime",))
        ts1.convert("uptime", "derive", "uptime_derive")
        ts1.convert("uptime", "percent", "uptime_percent")
        ts1.convert("uptime", "persecond", "uptime_persecond")
        print(ts1)
        # TODO: better testsdata to check if these conversion are correct
        ts2 = ts.slice(("com_select",))
        ts2.convert("com_select", "counter32", "com_select_counter32")
        ts2.convert("com_select", "counter64", "com_select_counter64")
        ts2.convert("com_select", "gauge32", "com_select_gauge32")
        ts2.convert("com_select", "counterreset", "com_select_counterreset")
        print(ts2)


    def test_add_calc_col_single(self):
        print("testing load, slove, add_calc_col_single, add_calc_col_full")
        with gzip.open(self.testfile, "rt") as infile:
            ts = Timeseries.load(infile)
        ts2 = ts.slice(("bytes_sent", "bytes_received"))
        ts2.add_calc_col_single("bytes_sent", "kbytes_sent", lambda a : a / 8)
        ts2.add_calc_col_single("bytes_received", "kbytes_received", lambda a : a / 8)
        ts2.add_calc_col_full("kbytes", lambda row : (row["bytes_sent"] + row["bytes_received"])/ 8)
        assert ts2[0][1] == ts2[0][3] * 8
        assert ts2[0][2] == ts2[0][4] * 8
        assert ts2[0][5] == ts2[0][3] + ts2[0][4]
        print(ts2)
        pass

    def test_remove_col(self):
        print("testing slice and remove_col")
        ts = self.app.slice(('com_select', 'uptime'))
        assert ts[0][2] == 926326.0
        ts.remove_col("com_select")
        assert ts[0][1] == 926326.0

    def pop(self, colnum):
        pass

    def test_append(self):
        print("testing get_series, append and slice")
        with gzip.open(self.testfile, "rt") as infile:
            ts = Timeseries.load(infile)
        series = ts.get_serie("com_select")
        ts1 = ts.slice(("com_select",))
        ts1.append("com_select_2", series)
        print(ts1)

    def test_load(self):
        print("testing load and slice")
        ts = Timeseries.load(gzip.open(self.testfile, "rt"))
        print(ts.slice(("bytes_sent",)))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
