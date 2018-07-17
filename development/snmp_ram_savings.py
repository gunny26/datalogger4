#!/usr/bin/python3
"""
program to find servers who do not use their memory, permanently

first:
for every day (x-days back) search for servers who are not using their memory above 30% on this day

second:
find servers who appear on first selction on more than 50% of the days

output:
servers who use their memory less than 30% on 50% of the days analyzed
be aware: this could also be an phyiscal server
"""
import datetime
import datalogger3
from datalogger3.b64 import b64eval

def get_vm_shortage(*args):
    datestring = args[0]
    ret_data = args[1]
    # get list of virtual hostnames
    project = "vicenter"
    tablename = "virtualMachineMemoryStats"
    dl.setup(project, tablename, datestring)
    virtual_machines = []
    for index_key in dl["tsastats"].keys():
        hostname = index_key[0].lower().split(".")[0]
        virtual_machines.append(hostname)
    project = "snmp"
    tablename = "hrStorageTable"
    dl.setup(project, tablename, datestring)
    # aggregate data
    data = {}
    for index_key in sorted(dl["tsastats"].keys()):
        hostname, hrStorageName, hrStorageType = index_key
        if hostname.lower().split(".")[0] not in virtual_machines: # use only virtual machines
            continue
        tsstats = dl["tsastats"][index_key]
        if "HOST-RESOURCES-TYPES::hrStorageVirtualMemory" == hrStorageType and "virtual memory" == hrStorageName.lower():
            if hostname not in data:
                data[hostname] = {}
            data[hostname]["virtual_size_gb"] = tsstats["hrStorageSize"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
            data[hostname]["virtual_used_pct"] = 100.0 * tsstats["hrStorageUsed"]["max"] / tsstats["hrStorageSize"]["max"]
            data[hostname]["virtual_used_gb"] = tsstats["hrStorageUsed"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
            try:
                data[hostname]["virtual_used_max_to_median"] = 100 * tsstats["hrStorageUsed"]["max"] / tsstats["hrStorageUsed"]["median"]
            except ZeroDivisionError:
                data[hostname]["virtual_used_max_to_median"] = -1.0
        if "HOST-RESOURCES-TYPES::hrStorageRam" == hrStorageType:
            if hostname not in data:
                data[hostname] = {}
            data[hostname]["ram_size_gb"] = tsstats["hrStorageSize"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
            data[hostname]["ram_used_pct"] = 100.0 * tsstats["hrStorageUsed"]["max"] / tsstats["hrStorageSize"]["max"]
            data[hostname]["ram_used_gb"] = tsstats["hrStorageUsed"]["max"] * tsstats["hrStorageAllocationUnits"]["max"] / 1024 / 1024 / 1024 # to get GB
    # evaluate data
    firstrow = True
    for hostname, entry in sorted(data.items()):
        if firstrow:
            print("\t".join(["hostname", "datestring"] + sorted(entry.keys())))
            firstrow = False
        try:
            if (entry["virtual_used_pct"] < 30.0 or entry["ram_used_pct"] < 30.0) and (entry["ram_size_gb"] > 2.0):
                print("\t".join([hostname, datestring] + ["%0.2f" % entry[key] for key in sorted(entry.keys())]))
                if hostname not in ret_data:
                    ret_data[hostname] = []
                ret_data[hostname].append(entry)
        except KeyError:
            pass

#
dl = datalogger3.DataLogger("/var/rrd")
print("showing virtual server with minimal ram usage, possible to save some GB")
ret_data = {}
days = 0
# use the last two weeks to get data
start = (datetime.date.today() - datetime.timedelta(days=14)).isoformat()
stop = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
for datestring in dl.datewalker(start, stop):
    print("-" * 80)
    print("Analysis for %s" % datestring)
    get_vm_shortage(datestring, ret_data)
    days += 1 
# aggregate data of all analyzed days
total_data = {}
for hostname, entries in ret_data.items():
    pct_found = 100 * len(entries) / days
    if pct_found > 50.0:
        total_data[hostname] = {
            "pct_found" : pct_found,
            "virtual_used_pct_max" : max((entry["virtual_used_pct"] for entry in entries)),
            "ram_used_gb_max" : max((entry["ram_used_gb"] for entry in entries)),
            "ram_used_pct_max" : max((entry["ram_used_pct"] for entry in entries)),
            "ram_size_gb_max" : max((entry["ram_size_gb"] for entry in entries)),
            "virtual_size_gb_max" : max((entry["virtual_size_gb"] for entry in entries)),
            "virtual_used_gb_max" : max((entry["virtual_used_gb"] for entry in entries)),
            "ram_savings_max" : max((entry["ram_size_gb"] for entry in entries)) - max(((entry["ram_size_gb"] * entry["ram_used_pct"] / 100) for entry in entries)),
            "ram_savings_min" : max((entry["ram_size_gb"] for entry in entries)) - min(((entry["ram_size_gb"] * entry["ram_used_pct"] / 100) for entry in entries)),
        }
# final output
print("showing servers which most often are found")
firstrow = True
for hostname, entry in total_data.items():
    if firstrow:
        print("\t".join(["hostname", ] + sorted(entry.keys())))
        firstrow = False
    print("\t".join([hostname, ] + ["%0.2f" % entry[key] for key in sorted(entry.keys())]))
