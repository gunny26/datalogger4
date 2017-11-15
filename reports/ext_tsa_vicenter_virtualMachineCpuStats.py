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

def extend(datestring):
    tsa = datalogger[datestring]
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
        new_index_key = tuple((index_key_dict[key1] for key1 in new_index_keys))
        logging.info("new index_key : %s", index_key_dict)
        # adding same data to new key
        tsa2[new_index_key] = tsa[key]
    return tsa2


if __name__ == "__main__":
    # VirtualCenterHelperWeb setup and CMDB Connection
    os.environ['NO_PROXY'] = 'tirol-kliniken.cc'
    URL = "http://sbapps.tirol-kliniken.cc/vsphere"
    APIKEY = "05faa0ca-038f-49fd-b4e9-6907ccd06a1f"
    webapp = VirtualCenterHelperWeb(URL, APIKEY)
    cmdb = Cmdb2WebClient.Cmdb2WebClient()
    # DataLogger stuff
    datalogger = DataLogger("/var/rrd", "vicenter", "virtualMachineCpuStats")
    datestring = DataLogger.get_last_business_day_datestring()
    tsa = extend(datestring)
    datalogger2 = DataLogger("/var/rrd", "vicenter", "virtualMachineCpuStats_extended")
    datalogger2.import_tsa(datestring, tsa)
