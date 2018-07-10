#!/usr/bin/python3
"""
program to find processes which are incresing in memory over one day

criterias:
- increase more than 10% of max
- max usage above 200MB
- above 140 (interval is 10 min) measures per day
"""
import json
import datetime
import datalogger3
from datalogger3.b64 import b64eval


def get_memory_leakers(*args):
    datestring = args[0]
    project = "snmp"
    tablename = "hrSWRunPerfTable"
    mem = "hrSWRunPerfMem"
    dl.setup(project, tablename, datestring)
    cache = dl["caches"]
    # aggregate data, use only server with more than 2 cores
    data = []
    for index_key_str in cache["tsstat"]["keys"].keys():
        index_key = eval(index_key_str)
        tsstats = dl["tsastats", index_key]
        if tsstats["hrSWRunPerfMem"]["count"] < 140:
            continue
        diff_kb = tsstats["hrSWRunPerfMem"]["diff"]
        if diff_kb < 0:
            continue
        max_kb = tsstats["hrSWRunPerfMem"]["max"]
        if max_kb < 512000: # abov aprox. half GB
            continue
        diff_pct = 100.0 * diff_kb / max_kb # more than 10%
        if diff_pct < 10.0:
            continue
        index_dict = dict(zip(dl.index_keynames, index_key))
        row_dict = {}
        row_dict.update(index_dict)
        row_dict["diff_kb"] = diff_kb
        row_dict["diff_pct"] = diff_pct
        row_dict["max_kb"] = max_kb
        row_dict["min_kb"] = tsstats["hrSWRunPerfMem"]["min"]
        data.append(row_dict)
        # print("\t".join(list(index_key) + [str(diff_kb), str(max_kb), str(diff_pct)]))
    headers = list(dl.index_keynames) + ["diff_pct", "diff_kb", "min_kb", "max_kb"]
    firstrow = True
    for row in sorted(data, key=lambda a: a["diff_pct"], reverse=True)[:40]:
        if firstrow is True:
            print("\t".join(headers))
            firstrow = False
        print("\t".join((str(row[header]) for header in headers)))

dl = datalogger3.DataLogger("/var/rrd")
print("showing servers with uneven CPU usage")
datestring = dl.get_last_business_day_datestring()
print("-" * 80)
print("Analysis for %s" % datestring)
get_memory_leakers(datestring)
