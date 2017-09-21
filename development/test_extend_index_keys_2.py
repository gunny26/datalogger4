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
from tilak_itsm import Cmdb2WebClient as Cmdb2WebClient
#from commons import *

def main(datestring):
    tsa = datalogger[datestring]
    logging.info(tsa.index_keys)
    logging.info(tsa.value_keys)
    logging.info(tsa.ts_key)
    logging.info(tsa.datatypes)
    new_index_keys = tsa.index_keys + ("esxhost", "cluster", "bs_name", "bs_company")
    tsa2 = TimeseriesArray.TimeseriesArrayLazy(new_index_keys, tsa.value_keys, tsa.ts_key, tsa.datatypes)
    for key in tsa.keys():
        index_key_dict = dict(zip(tsa.index_keys, key))
        # spice up with some virtual center data
        try:
            vm_moref = webapp.get_vm_moref_by_name(index_key_dict["hostname"])
            host_moref = webapp.get_host_by_vm(vm_moref)
            index_key_dict["esxhost"] = webapp.get_name_by_moref(host_moref[0])
            index_key_dict["cluster"] = webapp.get_cluster_by_vm(vm_moref)
        except KeyError:
            index_key_dict["esxhost"] = "not found"
            index_key_dict["cluster"] = "not_found"
        # try to find some CMDB Informations
        cs = cmdb.get_computersystem(index_key_dict["hostname"])
        if len(cs) > 0:
            # TODO: do not choose the first, choose the one with the
            # lowest depth !!!
            if len(cs["businessservices"]) > 0:
                index_key_dict["bs_name"] = cs["businessservices"][0]["name"]
                bs = cmdb.get_businessservice(cs["businessservices"][0]["instanceid"])
                index_key_dict["bs_company"] = bs["company"]
            else:
                index_key_dict["bs_name"] = "not modeled yet"
                index_key_dict["bs_company"] = "not modeled yet"
        else:
            index_key_dict["bs_name"] = "not in CMDB"
            index_key_dict["bs_company"] = "not in CMDB"
        logging.info("new index_key_dict: %s", index_key_dict)
        new_index_key = tuple((index_key_dict[key1] for key1 in new_index_keys))
        logging.info("new index_key : %s", index_key_dict)
        # adding same data to new key
        tsa2[new_index_key] = tsa[key]
    # store newly created tsa
    datalogger2 = DataLogger("/var/rrd", "vicenter", "virtualMachineCpuStats_extended")
    datalogger2.import_tsa(datestring, tsa2)
    tsastat = TimeseriesArrayStats(tsa2)
    tsastat_g = datalogger.tsastat_group_by(tsastat, ("bs_name", ))
    data = [tsastat_g.index_keynames + tsastat_g.value_keynames]
    for key in tsastat_g.keys():
        rowdata = key + ("%0.2f" % tsastat_g[key]["cpu.idle.summation"]["min"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["avg"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["sum"])
        logging.info(rowdata)
        data.append(rowdata)
    #json.dump(data, open(os.path.join(special_reports_dir, "sr_unused_cpu_by_esxhost.json"), "w"))


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
    cmdb = Cmdb2WebClient.Cmdb2WebClient()
    main(datestring)
    #cProfile.run("main()")
