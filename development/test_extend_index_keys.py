#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import os
import json
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArrayLazy as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import Timeseries as Timeseries
from tk_vch import VirtualCenterHelperWeb as VirtualCenterHelperWeb
#from commons import *

def main():
    tsa = datalogger["2016-09-09"]
    logging.info(tsa.index_keys)
    logging.info(tsa.value_keys)
    logging.info(tsa.ts_key)
    logging.info(tsa.datatypes)
    new_index_keys = tsa.index_keys + ("esxhost", "cluster")
    tsa2 = TimeseriesArray.TimeseriesArrayLazy(new_index_keys, tsa.value_keys, tsa.ts_key, tsa.datatypes)
    for key in tsa.keys():
        index_key_dict = dict(zip(tsa.index_keys, key))
        try:
            vm_moref = webapp.get_vm_moref_by_name(index_key_dict["hostname"])
            host_moref = webapp.get_host_by_vm(vm_moref)
            index_key_dict["esxhost"] = webapp.get_name_by_moref(vm_moref)
            index_key_dict["cluster"] = webapp.get_cluster_by_vm(vm_moref)
        except KeyError:
            index_key_dict["esxhost"] = "not found"
            index_key_dict["cluster"] = "not_found"
        logging.info("new index_key_dict: %s", index_key_dict)
        new_index_key = tuple((index_key_dict[key1] for key1 in new_index_keys))
        logging.info("new index_key : %s", index_key_dict)
        # adding same data to new key
        tsa2[new_index_key] = tsa[key]
    tsastat = TimeseriesArrayStats(tsa2)
    print(tsastat)

def sr_report_unused_cpu(datestring):
    special_reports_dir = os.path.join(datalogger.global_cachedir, datestring, "__special_reports")
    if not os.path.exists(special_reports_dir):
        os.mkdir(special_reports_dir)
    tsastat = datalogger.load_tsastats(datestring)
    # destination tsastat, like original but extended index_keynames
    tsastats_new = TimeseriesArrayStats.__new__(TimeseriesArrayStats)
    tsastats_new.index_keys = tsastat.index_keys + ("esxhost", "cluster")
    tsastats_new.value_keys = tsastat.value_keys
    tsastats_new.stats = {}
    for key, stats in tsastat.stats.items():
        key_dict = dict(zip(tsastat.index_keys, key))
        vm_moref = webapp.get_vm_moref_by_name(key_dict["hostname"])
        host_moref = webapp.get_host_by_vm(vm_moref)
        # try to get ESX Server
        try:
            key_dict["esxhost"] = webapp.get_name_by_moref(host_moref[0])
        except KeyError:
            key_dict["esxhost"] = "not found"
        # try to get cluster
        try:
            key_dict["cluster"] = webapp.get_cluster_by_vm(vm_moref)
        except KeyError:
            key_dict["cluster"] = "no cluster"
        new_index_key = tuple((key_dict[key1] for key1 in tsastats_new.index_keys))
        tsastats_new.stats[new_index_key] = stats
    # group by hostname
    tsastat_g = datalogger.tsastat_group_by(tsastats_new, ("hostname", "cluster", "esxhost"))
    data = [tsastat_g.index_keynames + tsastat_g.value_keynames]
    for key in tsastat_g.keys():
        data.append(key + ("%0.2f" % tsastat_g[key]["cpu.idle.summation"]["min"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["avg"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["max"]))
    json.dump(data, open(os.path.join(special_reports_dir, "sr_unused_cpu_by_hostname.json"), "w"))
    # group by cluster
    tsastat_g = datalogger.tsastat_group_by(tsastats_new, ("cluster", ))
    data = [tsastat_g.index_keynames + tsastat_g.value_keynames]
    for key in tsastat_g.keys():
        data.append(key + ("%0.2f" % tsastat_g[key]["cpu.idle.summation"]["min"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["avg"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["max"]))
    json.dump(data, open(os.path.join(special_reports_dir, "sr_unused_cpu_by_cluster.json"), "w"))
    # group by esxhost
    tsastat_g = datalogger.tsastat_group_by(tsastats_new, ("esxhost", ))
    data = [tsastat_g.index_keynames + tsastat_g.value_keynames]
    for key in tsastat_g.keys():
        data.append(key + ("%0.2f" % tsastat_g[key]["cpu.idle.summation"]["min"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["avg"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["max"]))
    json.dump(data, open(os.path.join(special_reports_dir, "sr_unused_cpu_by_esxhost.json"), "w"))


if __name__ == "__main__":
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = DataLogger.get_last_business_day_datestring()
    # VirtualCenterHelperWeb setup
    os.environ['NO_PROXY'] = 'tirol-kliniken.cc'
    URL = "http://sbapps.tirol-kliniken.cc/vsphere"
    APIKEY = "05faa0ca-038f-49fd-b4e9-6907ccd06a1f"
    webapp = VirtualCenterHelperWeb(URL, APIKEY)
    sr_report_unused_cpu(datestring)
    #main()
    #cProfile.run("main()")
