#!/usr/bin/python -3
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import Timeseries as Timeseries

def group_tsastat(tsastat, groupby):
    # how to aggregate statistical values
    group_funcs = {
        u'count' : lambda a, b : a + b,
        u'std' : lambda a, b : (a + b)/2,
        u'avg': lambda a, b : (a + b)/2,
        u'last' : lambda a, b : -1.0, # theres no meaning
        u'min' : min,
        u'max' : max,
        u'sum' : lambda a, b : (a + b) / 2,
        u'median' : lambda a, b : (a + b)/2,
        u'mean' : lambda a, b : (a + b)/2,
        u'diff' : lambda a, b : (a + b)/2,
        u'dec' : lambda a, b : (a + b)/2,
        u'inc' : lambda a, b : (a + b)/2,
        u'first' : lambda a, b : -1.0, # theres no meaning
    }
    #tsastat = datalogger.load_tsastats("2016-02-08")
    #print(datalogger.index_keynames)
    #print(datalogger.value_keynames)
    #groupby = ("hostname", )
    newdata = {}
    for index_key, tsstat in tsastat.items():
        #print("index_key :", index_key)
        key_dict = dict(zip(datalogger.index_keynames, index_key))
        newkey = tuple([key_dict[key] for key in groupby])
        #print("grouped key: ", newkey)
        if newkey not in newdata:
            #print("first appearance of this index_key")
            newdata[newkey] = {}
        for value_key in datalogger.value_keynames:
            if value_key not in newdata[newkey]:
                #print("first appearance of this value_key")
                newdata[newkey][value_key] = dict(tsstat[value_key])
            else:
                #print("grouping data")
                for stat_funcname in tsstat[value_key].keys():
                    #print("statistical function: ", stat_funcname)
                    existing = float(newdata[newkey][value_key][stat_funcname])
                    #print("existing data: ", existing)
                    to_group = float(tsstat[value_key][stat_funcname])
                    #print("to add data  : ", to_group)
                    newdata[newkey][value_key][stat_funcname] = group_funcs[stat_funcname](existing, to_group)
                    #print("calculated value: ", newdata[newkey][value_key][stat_funcname])
    return newdata

def main():
    tsastat = datalogger.load_tsastats("2016-02-08")
    tsastat_g = group_tsastat(tsastat, ("hostname", ))
    data = []
    data.append(("hostname", "avg_idle_min", "avg_used_avg", "avg_used_max"))
    for key in tsastat_g.keys():
        num_cpu = sum([key[0] in index_key for index_key in tsastat.keys()])
        if num_cpu < 3 :
            continue
        #print("%s : %s" % (key[0], tsastat_g[key]["cpu.idle.summation"]["min"]))
        data.append((key[0], str(tsastat_g[key]["cpu.idle.summation"]["min"]), str(tsastat_g[key]["cpu.used.summation"]["avg"]), str(tsastat_g[key]["cpu.used.summation"]["max"])))
    for row in data:
        print("\t".join(row))

def main_old():
    datestring = "2016-02-08"
    tsa = datalogger["2016-02-08"]
    print(tsa.keys())
    tsa_g = datalogger.group_by(datestring, tsa, ("hostname", ), lambda a, b: a + b / 2)
    print(tsa_g.keys())
    tsastat_g = TimeseriesArrayStats(tsa_g)
    data = []
    data.append(("hostname", "avg_idle_min", "avg_used_avg", "avg_used_max"))
    for key in tsa_g.keys():
        num_cpu = sum([key[0] in index_key for index_key in tsa.keys()])
        if num_cpu < 3 :
            continue
        print("%s : %s" % (key[0], tsastat_g[key]["cpu.idle.summation"]["min"]))
        data.append((key[0], tsastat_g[key]["cpu.idle.summation"]["min"], tsastat_g[key]["cpu.used.summation"]["avg"], tsastat_g[key]["cpu.used.summation"]["max"]))
    for row in data:
        print("\t".join(row))

if __name__ == "__main__":
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = DataLogger.get_last_business_day_datestring()
    #main()
    cProfile.run("main()")
