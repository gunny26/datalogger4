#!/usr/bin/python3
"""
program to find servers who are short in virtual memory

output:
virtual servers who use >90% max of virtual memory and virtual memory max is above phyiscal memory
"""
import datetime
import datalogger3
from datalogger3.b64 import b64eval

def get_ipstor_appliance(*args):
    datestring = args[0]
    project = "ipstor"
    tablename = "vrsClientTable"
    dl.setup(project, tablename, datestring)
    tsastats = dl["tsastats"].stats
    # aggregate data
    data = {}
    for index_key, tsstats in dl["tsastats"].stats.items():
        hostname, vrsClientName, vrsclVirResourceName = index_key
        if hostname not in data:
            data[hostname] = {
                "vrsclSCSIReadCmd" : tsstats["vrsclSCSIReadCmd"]["avg"] if tsstats["vrsclSCSIReadCmd"]["avg"] > 0.0 else 0.0,
                "vrsclSCSIWriteCmd" : tsstats["vrsclSCSIWriteCmd"]["avg"] if tsstats["vrsclSCSIWriteCmd"]["avg"] > 0.0 else 0.0,
                "vrsclKBRead64" : tsstats["vrsclKBRead64"]["avg"] if tsstats["vrsclKBRead64"]["avg"] > 0.0 else 0.0,
                "vrsclKBWritten64" : tsstats["vrsclKBWritten64"]["avg"] if tsstats["vrsclKBWritten64"]["avg"] > 0.0 else 0.0,
            }
        else:
            data[hostname]["vrsclSCSIReadCmd"] += tsstats["vrsclSCSIReadCmd"]["avg"] if tsstats["vrsclSCSIReadCmd"]["avg"] > 0.0 else 0.0
            data[hostname]["vrsclSCSIWriteCmd"] += tsstats["vrsclSCSIWriteCmd"]["avg"] if tsstats["vrsclSCSIWriteCmd"]["avg"] > 0.0 else 0.0
            data[hostname]["vrsclKBRead64"] += tsstats["vrsclKBRead64"]["avg"] if tsstats["vrsclKBRead64"]["avg"] > 0.0 else 0.0
            data[hostname]["vrsclKBWritten64"] += tsstats["vrsclKBWritten64"]["avg"] if tsstats["vrsclKBWritten64"]["avg"] > 0.0 else 0.0
    return data

dl = datalogger3.DataLogger("/var/rrd")
print("showing server with max virtual memory usage over 90% and over physical memory size")
firstrow = True
for datestring in dl.datewalker("2016-01-01", "2018-07-04"):
    #print("-" * 80)
    #print("Analysis for %s" % datestring)
    data = get_ipstor_appliance(datestring)
    # evaluate data
    for hostname, entry in data.items():
        if firstrow:
            print("\t".join(["hostname", "datestring"] + sorted(entry.keys())))
            firstrow = False
        print("\t".join([hostname, datestring] + ["%0.2f" % entry[key] for key in sorted(entry.keys())]))


