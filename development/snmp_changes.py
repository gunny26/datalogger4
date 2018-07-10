#!/usr/bin/python3
"""
program to find servers who are short in virtual memory

output:
virtual servers who use >90% max of virtual memory and virtual memory max is above phyiscal memory
"""
import datetime
import logging
logging.basicConfig(level=logging.INFO)
import datalogger3
from datalogger3.b64 import b64eval

def get_vm_shortage(*args):
    datestring = args[0]
    project = "snmp"
    tablename = "hrStorageTable"
    dl.setup(project, tablename, datestring)
    # tsastats = dl["tsastats"].stats
    # aggregate data
    data = {}
    for index_key in dl["tsastats"].keys():
        hostname, hrStorageName, hrStorageType = index_key
        if hrStorageType in ("HOST-RESOURCES-TYPES::hrStorageOther", "HOST-RESOURCES-TYPES::hrStorageNetworkDisk", "HOST-RESOURCES-TYPES::hrStorageVirtualMemory", "HOST-RESOURCES-TYPES::hrStorageRemovableDisk"):
            continue
        if hrStorageName.startswith("/run") or hrStorageName.startswith("/dev") or hrStorageName.startswith("/sys"):
            continue
        tsstats = dl["tsastats"][index_key]
        size = int(tsstats["hrStorageSize"]["last"] * tsstats["hrStorageAllocationUnits"]["last"] / 1024 / 1024)
        data[index_key] = size
    return data

dl = datalogger3.DataLogger("/var/rrd")
print("showing server with max virtual memory usage over 90% and over physical memory size")
lastdata = None
for datestring in dl.datewalker("2018-01-01", "2018-07-08"):
    print("-" * 80)
    print("Analysis for %s" % datestring)
    if lastdata is None:
        lastdata = get_vm_shortage(datestring)
    else:
        newdata = get_vm_shortage(datestring)
        for index_key in newdata:
            if index_key not in lastdata:
                #print("ADDED: %s : %s" % (index_key, newdata[index_key]))
                pass
            else:
                if newdata[index_key] != lastdata[index_key]:
                    print("CHANGED: %s from %s to %s" % (index_key, lastdata[index_key], newdata[index_key]))
                    # pass
        for index_key in lastdata:
            if index_key not in newdata:
                #print("DELETED: %s : %s" % (index_key, lastdata[index_key]))
                pass
        lastdata = newdata
