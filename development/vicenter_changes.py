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

def get_vicenter_memory_size(*args):
    datestring = args[0]
    data = args[1]
    project = "vicenter"
    tablename = "virtualMachineMemoryStats"
    dl.setup(project, tablename, datestring)
    # aggregate data
    for index_key, tsstats in dl["tsastats"].stats.items():
        hostname, instance = index_key
        size = int(tsstats["mem.granted.average"]["max"] / 1024 / 1024)
        if hostname not in data:
            data[hostname] = {
                "cpu": 0,
                "mem": None
            }
        data[hostname]["mem"] = size
    return data

def get_vicenter_cpu_count(*args):
    datestring = args[0]
    data = args[1]
    project = "vicenter"
    tablename = "virtualMachineCpuStats6"
    dl.setup(project, tablename, datestring)
    # aggregate data
    for index_key, tsstats in dl["tsastats"].stats.items():
        hostname, instance = index_key
        if hostname not in data:
            data[hostname] = {
                "cpu": 0,
                "mem": None
            }
        data[hostname]["cpu"] += 1
    return data

dl = datalogger3.DataLogger("/var/rrd")
print("showing server with max virtual memory usage over 90% and over physical memory size")
lastdata = None
start = (datetime.date.today() - datetime.timedelta(weeks=4)).isoformat()
stop = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
for datestring in dl.datewalker(start, stop):
    print("-" * 80)
    print("Analysis for %s" % datestring)
    if lastdata is None:
        lastdata = {}
        get_vicenter_memory_size(datestring, lastdata)
        get_vicenter_cpu_count(datestring, lastdata)
    else:
        newdata = {}
        get_vicenter_memory_size(datestring, newdata)
        get_vicenter_cpu_count(datestring, newdata)
        for index_key in newdata:
            if index_key not in lastdata:
                #print("ADDED: %s : %s" % (index_key, newdata[index_key]))
                pass
            else:
                if newdata[index_key] != lastdata[index_key]:
                    print("CHANGED: %s from %s to %s" % (index_key, lastdata[index_key], newdata[index_key]))
        for index_key in lastdata:
            if index_key not in newdata:
                #print("DELETED: %s : %s" % (index_key, lastdata[index_key]))
                pass
        lastdata = newdata
