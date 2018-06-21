#!/usr/bin/python3
"""
program to find virtual server who use their cores not evenly
possible this is a single core application on this server

algorithm:
1.
aggregate all cores for one particulate virtual machine
use only virtual machones with more than 2 cores

2.
use cpu.used.summation - sum of this day and calculate max and std of all cores
if std in percent of max is aboce 10% - add to final result

virtual machine with top std_pct using ther cores most unevenly
"""
import datetime
import statistics
import datalogger3
from datalogger3.b64 import b64eval

def get_cpu_singlecore(*args):
    datestring = args[0]
    project = "vicenter"
    tablename = "virtualMachineCpuStats6"
    dl.setup(project, tablename, datestring)
    tsastats = dl["tsastats"].stats
    # aggregate data, use only server with more than 2 cores
    data = {}
    for index_key, tsstats in dl["tsastats"].stats.items():
        hostname = index_key[0]
        if hostname not in data:
            data[hostname] = []
        data[hostname].append(tsstats)
    data2 = dict([(hostname, entry) for hostname, entry in data.items() if len(entry) > 1])
    data3 = {}
    for hostname, entry in data2.items():
        #print(hostname)
        #for index, core in enumerate(entry):
        #    print("\t%d : %d" % (index, core["cpu.used.summation"]["sum"]))
        max_used = max((core["cpu.used.summation"]["sum"] for core in entry))
        #print("\tmax : %0.2f" % max_used)
        std = statistics.stdev((core["cpu.used.summation"]["sum"] for core in entry))
        #print("\tstd : %0.2f" % std)
        std_pct = 100.0 * std / max_used
        #print("\tstd : %0.2f %%" % std_pct)
        if std_pct > 10.0:
            if hostname not in data3:
                data3[hostname] = {
                    "max_used" : max_used,
                    "std" : std,
                    "std_pct" : std_pct
                }
    firstrow = True
    for hostname, entry in data3.items():
        if firstrow:
            print("\t".join(["hostname", ] + [key for key in sorted(entry.keys())]))
            firstrow = False
        print("\t".join([hostname, ] + ["%0.2f" % entry[key] for key in sorted(entry.keys())]))
#
dl = datalogger3.DataLogger("/var/rrd")
print("showing servers with uneven CPU usage")
for datestring in dl.datewalker("2018-05-01", "2018-05-31"):
    print("-" * 80)
    print("Analysis for %s" % datestring)
    get_cpu_singlecore(datestring)
