#!/usr/bin/python3
"""
program to aggregate all running processes for one day
by program name

agregated value:
sum(sum(hrSWRunPerfCPU))
sum(avg(hrSWRunPerfMem))

first sum, by timeseries, second sum by all timeseries for this day and programname
"""
import json
import datetime
import datalogger3
from datalogger3.b64 import b64eval

def tsstats_2d(tsa, value_keyname=None, stat_func_name=None):
    assert value_keyname != stat_func_name
        
def index_dict(index_key, index_keynames):
    return dict(zip(index_keynames, index_key))


def get_hrSWRunPerfTable_grouped(*args):
    datestring = args[0]
    project = "snmp"
    tablename = "hrSWRunPerfTable"
    dl.setup(project, tablename, datestring)
    # aggregate data, use only server with more than 2 cores
    data = {}
    for index_key in dl["tsastats"].keys():
        #if tsstats["hrSWRunPerfCPU"]["count"] < 140:
        #    continue
        if "System Idle Process" in index_key:
            continue
        tsstats = dl["tsastats"][index_key]
        # helpers
        index_dict = dict(zip(dl.index_keynames, index_key))
        name = index_dict["hrSWRunName"]
        perf_cpu_sum = tsstats["hrSWRunPerfCPU"]["sum"] 
        perf_mem_avg = tsstats["hrSWRunPerfMem"]["avg"]
        # print(name, perf_cpu_sum, perf_mem_avg)
        if name not in data:
            data[name] = {
                "hrSWRunName" : name,
                "#num_found" : 1,
                "hostnames" : [index_dict["hostname"], ],
                "cpu_sum_sum" : perf_cpu_sum,
                "mem_avg_sum" : perf_mem_avg
            }
        else:
            try:
                data[name]["#num_found"] += 1
                data[name]["cpu_sum_sum"] += perf_cpu_sum
                data[name]["mem_avg_sum"] += perf_mem_avg
                if index_dict["hostname"] not in data[index_dict["hrSWRunName"]]["hostnames"]:
                    data[name]["hostnames"].append(index_dict["hostname"])
            except TypeError as exc:
                print(json.dumps(data[name], indent=4))
                print(perf_cpu_sum)
                raise exc
    # calculate number of different hostnames this process is running
    for key in data:
        data[key]["#num_hostnames"] = len(data[key]["hostnames"])
    firstrow = True
    headers = ["hrSWRunName", "#num_found", "#num_hostnames", "cpu_sum_sum", "mem_avg_sum"]
    for row in sorted(data.values(), key=lambda a: a["cpu_sum_sum"], reverse=True)[:100]:
        if firstrow is True:
            print("\t".join(headers))
            firstrow = False
        print("\t".join((str(row[header]) for header in headers)))

dl = datalogger3.DataLogger("/var/rrd")
datestring = dl.get_last_business_day_datestring()
print("showing servers with uneven CPU usage")
print("-" * 80)
print("Analysis for %s" % datestring)
get_hrSWRunPerfTable_grouped(datestring)
