#!/usr/bin/python3

import unittest
import logging
import datetime
import gzip
import os
import json
# own modules
from datalogger4.Timeseries import Timeseries
from datalogger4.TimeseriesStats import TimeseriesStats


class Test(unittest.TestCase):


    def setUp(self):
        self.basedir = "/var/rrd"
        self.datestring = "2015-11-30"
        self.testfile = "testdata/ts_KHUnc3J2d2Vic3FsMi50aWxhay5jYycsKQ==.csv.gz"
        with gzip.open(self.testfile, "rt") as infile:
            ts = Timeseries.load(infile)
        self.tsstat = TimeseriesStats(ts)

    def test__eq__(self):
        with gzip.open(self.testfile, "rt") as infile:
            ts = Timeseries.load(infile)
        tsstat = TimeseriesStats(ts)
        assert tsstat == self.tsstat

    def test__getitem__(self):
        assert self.tsstat[("opened_tables", "min")] == 3581.000000
        assert self.tsstat["opened_tables"] == {'min': 3581.0, 'max': 3601.0, 'avg': 3594.0, 'sum': 1035072.0, 'std': 7.340905181848413, 'median': 3599.0, 'count': 288, 'first': 3581.0, 'last': 3601.0, 'mean': 3594.0, 'inc': 20.0, 'dec': 0.0, 'diff': 20.0}

    def test__str__(self):
        print(self.tsstat)

    def test_keys(self):
        assert list(self.tsstat.keys()) == ['com_select', 'uptime', 'com_insert', 'slow_queries', 'bytes_sent', 'com_update', 'connections', 'com_delete', 'qcache_hits', 'questions', 'opened_tables', 'aborted_connects', 'bytes_received', 'created_tmp_tables', 'created_tmp_disk_tables', 'aborted_clients']

    def test_values(self):
        assert list(self.tsstat.values())[0]["min"] == 89169365.0

    def test_items(self):
        for key, value in self.tsstat.items():
            assert isinstance(key, str)
            assert isinstance(value, dict)

    def test_stats(self):
        assert isinstance(self.tsstat.stats, dict)

    def test_funcnames(self):
        assert list(self.tsstat.funcnames) == ['avg', 'count', 'dec', 'diff', 'first', 'inc', 'last', 'max', 'mean', 'median', 'min', 'std', 'sum']

    def test_get_stats(self):
        assert isinstance(self.tsstat.get_stats(), dict)

    def test_get_stat(self):
        assert self.tsstat.get_stat("min")['com_select'] == 89169365.0

    def test_dump(self):
        with open("testdata/tsstat_testdump.json", "wt") as outfile:
            self.tsstat.dump(outfile)
        with open("testdata/tsstat_testdump.json", "rt") as infile:
            tsstat = TimeseriesStats.load(infile)
        assert self.tsstat == tsstat

    def test_load(self):
        with open("testdata/tsstat_KHUnc3J2d2Vic3FsMi50aWxhay5jYycsKQ==.json", "rt") as infile:
            tsstat = TimeseriesStats.load(infile)
        assert tsstat[("com_select", "min")] == 0.000000

    def test_to_json(self):
        assert "\"diff\": 10961584.0" in self.tsstat.to_json()

    def test_to_data(self):
        data = self.tsstat.to_data()
        assert data["aborted_clients"]["count"] == 288
        data_str = json.dumps(data, indent=4)
        assert isinstance(data_str, str)

    def test_from_json(self):
        tsstat = TimeseriesStats.from_json(self.tsstat.to_json())
        assert tsstat == self.tsstat

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
