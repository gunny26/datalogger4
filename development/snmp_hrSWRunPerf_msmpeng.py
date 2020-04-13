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
    value_keyname = "hrSWRunPerfCPU"
    dl.setup(project, tablename, datestring)
    # cache = dl["caches"]
    # aggregate data, use only server with more than 2 cores
    data = []
    for index_key in dl["tsastats"].keys():
        # index_key = eval(index_key_str)
        tsstats = dl["tsastats"][index_key]
        index_dict = dict(zip(dl.index_keynames, index_key))
        if index_dict["hrSWRunName"] == "System Idle Process":
            continue
        if index_dict["hrSWRunName"] != "MsMpEng.exe":
            continue
        #if index_dict["hostname"] != "srvskimvic01.tilak.cc":
        #    continue
        print(index_dict)
        row_dict = {}
        row_dict.update(index_dict)
        row_dict[value_keyname] = tsstats[value_keyname]["sum"]
        data.append(row_dict)
        # print("\t".join(list(index_key) + [str(diff_kb), str(max_kb), str(diff_pct)]))
    total_sum = sum((entry[value_keyname] for entry in data))
    for entry in data:
        entry["sum_pct"] = 100 * entry[value_keyname] / total_sum
    headers = list(dl.index_keynames) + [value_keyname, "sum_pct" ]
    firstrow = True
    for row in sorted(data, key=lambda a: a[value_keyname], reverse=True)[:40]:
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
