#!/usr/bin/python3
import json
import time
import concurrent.futures
# own modules
import datalogger3


def add_calc_col(dl, index_key, value_keyname, func):
    dl["tsa"][index_key].add_calc_col_full(value_keyname, func)


def tsa_add_calc_col(dl, value_keyname, func):
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for index_key in dl["tsa"].keys():
            futures.append(executor.submit(add_calc_col, dl, index_key, value_keyname, func))
        for future in concurrent.futures.as_completed(futures): # wait for futures to complete
            future.result()

dl = datalogger3.DataLogger("/var/rrd")
dl.setup("snmp", "hrStorageTable", "2018-07-23")
print(json.dumps(dl.meta, indent=4))
#for value_keyname in dl.meta["calc_col"]:
#    tsa_add_calc_col(dl, value_keyname, eval(dl.meta["calc_col"][value_keyname]))
#
index_key = ('srvcl14db2.tilak.cc', 'D:\\\\ Label:DB1_Data1  Serial Number 4ab46a37', 'HOST-RESOURCES-TYPES::hrStorageFixedDisk')
#print(dl["tsa"][index_key])
#for value_keyname in dl.meta["calc_col"]:
#    print("calculating %s" % value_keyname)
#    f = eval(dl.meta["calc_col"][value_keyname])
#    dl["tsa"][index_key].add_calc_col_full(value_keyname, f)
#print(dl["tsa"][index_key])
dl["tsa"].cache = True
for value_keyname in dl.meta["calc_col"]:
    print("calculating %s" % value_keyname)
    f = eval(dl.meta["calc_col"][value_keyname])
    dl["tsa"].add_calc_col_full(value_keyname, f)
print(dl["tsa"][index_key])
