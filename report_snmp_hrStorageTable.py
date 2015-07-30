#!/usr/bin/python
import logging
logging.basicConfig(level=logging.DEBUG)
from tilak_datalogger import DataLogger as DataLogger
from commons import *

def calc_hrStorageSizeUsage(data):
    try:
        return 100 * data[u"hrStorageUsed"] / data[u"hrStorageSize"]
    except ZeroDivisionError:
        return(-1)

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.read_tsa_full(datestring)
    for key in tsa.keys():
        if "HOST-RESOURCES-TYPES::hrStorageFixedDisk" in key:
            for subkey in key:
                if subkey.startswith("/dev") or subkey.startswith("/sys") or subkey.startswith("/proc") or subkey.startswith("/run"):
                    del(tsa[key])
            continue
        else:
            del(tsa[key])
    tsa.add_calc_col_full("hrStorageSizeUsage", calc_hrStorageSizeUsage)
    tsa.add_per_s_col("hrStorageUsed", "hrStorageUsed_s")
    # get rid of unusable data
    tsa.sanitize()
    # for grouped reports, reduce number of cols
    tsa_grouped = tsa.slice(('hrStorageUsed_s', ))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
