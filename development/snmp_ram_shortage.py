#!/usr/bin/python3
"""
program to find servers who are short in virtual memory

output:
virtual servers who use >90% max of virtual memory and virtual memory max is above phyiscal memory
"""
import datetime
import datalogger3
from datalogger3.b64 import b64eval

def get_vm_shortage(*args):
    datestring = args[0]
    project = "snmp"
    tablename = "hrStorageTable"
    dl.setup(project, tablename, datestring)
    # aggregate data
    data = {}
    for index_key, tsstats in dl["tsastats"].stats.items():
        if "HOST-RESOURCES-TYPES::hrStorageVirtualMemory" in index_key:
            tsstats = dl["tsastats"][index_key]
            hostname = index_key[0] # TODO: use some dict index_key
            if hostname not in data:
                data[hostname] = {}
            data[hostname]["virtual_size_gb"] = tsstats["hrStorageSize"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
            data[hostname]["virtual_used_pct"] = 100.0 * tsstats["hrStorageUsed"]["max"] / tsstats["hrStorageSize"]["max"]
            data[hostname]["virtual_used_gb"] = tsstats["hrStorageUsed"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
            try:
                data[hostname]["virtual_used_max_to_median"] = 100 * tsstats["hrStorageUsed"]["max"] / tsstats["hrStorageUsed"]["median"]
            except ZeroDivisionError:
                data[hostname]["virtual_used_max_to_median"] = -1.0
        if "HOST-RESOURCES-TYPES::hrStorageRam" in index_key:
            tsstats = dl["tsastats"][index_key]
            hostname = index_key[0] # TODO: use some dict index_key
            if hostname not in data:
                data[hostname] = {}
            data[hostname]["ram_size_gb"] = tsstats["hrStorageSize"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
            data[hostname]["ram_used_pct"] = 100.0 * tsstats["hrStorageUsed"]["max"] / tsstats["hrStorageSize"]["max"]
            data[hostname]["ram_used_gb"] = tsstats["hrStorageUsed"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
    # evaluate data
    firstrow = True
    for hostname, entry in data.items():
        if firstrow:
            print("\t".join(["hostname", "datestring"] + sorted(entry.keys())))
            firstrow = False
        if entry["virtual_used_pct"] > 90.0 and entry["virtual_used_gb"] > entry["ram_size_gb"]:
            print("\t".join([hostname, datestring] + ["%0.2f" % entry[key] for key in sorted(entry.keys())]))

#
dl = datalogger3.DataLogger("/var/rrd")
print("showing server with max virtual memory usage over 90% and over physical memory size")
for datestring in dl.datewalker("2018-07-01", "2018-07-09"):
    print("-" * 80)
    print("Analysis for %s" % datestring)
    get_vm_shortage(datestring)
