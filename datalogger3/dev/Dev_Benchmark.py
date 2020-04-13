#!/usr/bin/python3
from __future__ import print_function
import logging
logging.basicConfig(level=logging.INFO)
import json
import os
import cProfile
import pstats
# own modules
from DataLogger import DataLogger as DataLogger
from Timeseries import Timeseries as Timeseries
from TimeseriesArray import TimeseriesArray as TimeseriesArray
from TimeseriesStats import TimeseriesStats as TimeseriesStats
from TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats
from Quantile import QuantileArray as QuantileArray
from Quantile import Quantile as Quantile


def run():
    print("testing __getitem__")
    for i in range(10):
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        dl.delete_caches()
        caches = dl["caches"]
        assert isinstance(caches, dict)
        tsa = dl["tsa"]
        assert isinstance(tsa, TimeseriesArray)
        for key in tsa.keys():
            ts = dl["tsa", key]
            assert isinstance(ts, Timeseries)
        tsastats = dl["tsastats"]
        assert isinstance(tsastats, TimeseriesArrayStats)
        for key in tsastats.keys():
            tsstats = dl["tsastats", key]
            assert isinstance(tsstats, TimeseriesStats)
        qa = dl["qa"]
        assert isinstance(qa, QuantileArray)
        for key in qa.keys():
            quantile = dl["qa", key]
            assert isinstance(quantile, dict)
        total_stats = dl["total_stats"]
        assert total_stats["bytes_received"]["total_count"] == 5.0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cProfile.run("run()", "Dev_Benchmark.stats")
    p = pstats.Stats('Dev_Benchmark.stats')
    p.strip_dirs().sort_stats("time").print_stats()
