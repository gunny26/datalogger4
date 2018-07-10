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

        
def index_dict(index_key, index_keynames):
    return dict(zip(index_keynames, index_key))


def get_memory_leakers(*args):
    datestring = args[0]
    project = "snmp"
    tablename = "hrSWRunPerfTable"
    keyname = "hrSWRunPerfCPU"
    dl.setup(project, tablename, datestring)
    # aggregate data, use only server with more than 2 cores
    data = {}
    print(dl.index_keynames)
    print(dl.value_keynames)
    stat_func_name = "sum"
    headers = list(dl.index_keynames) + list(dl.value_keynames)
    print(headers)
    data = []
    for index_key in dl["tsastats"].keys():
        if "System Idle Process" in index_key:
            continue
        tsstats = dl["tsastats"][index_key]
        if tsstats[keyname]["count"] < 140:
            continue
        index_dict = dict(zip(dl.index_keynames, index_key))
        value_dict = dict(((value_keyname, tsstats[value_keyname][stat_func_name]) for value_keyname in dl.value_keynames)) 
        row_dict = {}
        row_dict.update(index_dict)
        row_dict.update(value_dict)
        data.append(row_dict)
    firstrow = True
    for row in sorted(data, key=lambda a: a["hrSWRunPerfCPU"], reverse=True)[:40]:
        if firstrow is True:
            print("\t".join(headers))
            firstrow = False
        print("\t".join((str(row[header]) for header in headers)))

dl = datalogger3.DataLogger("/var/rrd")
datestring = dl.get_last_business_day_datestring()
print("showing servers with uneven CPU usage")
print("-" * 80)
print("Analysis for %s" % datestring)
get_memory_leakers(datestring)
