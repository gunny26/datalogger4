#!/usr/bin/python3
"""
program to find ISL Connections between SAn Switchports

compare fcIfC3InFrames to every other port fcidC3OutFrames
if they match (sum of day, and std) in some percentag difference
this could be ISL connections
"""
import datetime
import statistics
import datalogger3
from datalogger3.b64 import b64eval


def compare(value1, value2, delta):
    return value2 * (1.0 - delta)  < value1 < value2 * (1.0 + delta)

def compare_tsstats(value1, value2, delta):
    return compare(value1["sum"], value2["sum"], delta) and compare(value1["std"], value2["std"], delta)

def get_sanportperf_network(*args):
    datestring = args[0]
    project = "sanportperf"
    tablename = "fcIfC3AccountingTable"
    left = "fcIfC3InFrames"
    right = "fcIfC3InFrames"
    dl.setup(project, tablename, datestring)
    tsastats = dl["tsastats"].stats
    # aggregate data, use only server with more than 2 cores
    data = {}
    for index_key, tsstats in dl["tsastats"].stats.items():
        hostname = index_key[0]
        if tsstats[left]["sum"] == 0.0:
            continue
        for index_key2, tsstats2 in dl["tsastats"].stats.items():
            if index_key2 == index_key: # comarison to self is useless
                continue
            if index_key[0] == index_key2[0]: # no ISL
                continue
            if index_key2[0][:3] != index_key[0][:3]: # fca to fca not fcb
                continue
            if compare_tsstats(tsstats[left], tsstats2[right], 0.01):
                print("Found ISL from %s to %s" % ("/".join(index_key), "/".join(index_key2)))
                # print(tsstats[left]["sum"], tsstats2[right]["sum"])

dl = datalogger3.DataLogger("/var/rrd")
print("showing servers with uneven CPU usage")
for datestring in dl.datewalker("2018-05-01", "2018-05-31"):
    print("-" * 80)
    print("Analysis for %s" % datestring)
    get_sanportperf_network(datestring)
    break
