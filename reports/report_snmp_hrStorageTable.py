#!/usr/bin/python
import logging
logging.basicConfig(level=logging.DEBUG)
from datalogger import DataLogger as DataLogger
from commons import *

def calc_hrStorageSizeUsage(data):
    try:
        return 100 * data[u"hrStorageUsed"] / data[u"hrStorageSize"]
    except ZeroDivisionError:
        return(-1)

def calc_hrStorageSizeKb(data):
    try:
        return data[u"hrStorageSize"] * data[u"hrStorageAllocationUnits"] / 1024
    except ZeroDivisionError:
        return(-1)

def calc_hrStorageUsedKb(data):
    try:
        return data[u"hrStorageUsed"] * data[u"hrStorageAllocationUnits"] / 1024
    except ZeroDivisionError:
        return(-1)

def calc_hrStorageFreeKb(data):
    try:
        return (data[u"hrStorageSize"] - data[u"hrStorageUsed"]) * data[u"hrStorageAllocationUnits"] / 1024
    except ZeroDivisionError:
        return(-1)


def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
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
    # for grouped reports, reduce number of cols
    tsa_grouped = tsa.slice(('hrStorageUsed_s', ))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def report_ram(datalogger, datestring):
    """
    special version for Physical RAM Usage
    """
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa.remove_col("hrStorageAllocationFailures")
    tsa.remove_col("hrStorageIndex")
    for key in tsa.keys():
        if not ("HOST-RESOURCES-TYPES::hrStorageRam" in key):
            del(tsa[key])
    tsa.add_calc_col_full("hrStorageSizeKb", calc_hrStorageSizeKb)
    tsa.add_calc_col_full("hrStorageUsedKb", calc_hrStorageUsedKb)
    tsa.add_calc_col_full("hrStorageFreeKb", calc_hrStorageFreeKb)
    tsa.add_calc_col_full("hrStorageUsagePct", calc_hrStorageSizeUsage)
    tsa.remove_col(u"hrStorageSize")
    tsa.remove_col(u"hrStorageUsed")
    tsa.remove_col(u"hrStorageAllocationUnits")
    # for grouped reports, reduce number of cols
    tsa_grouped = tsa.slice(('hrStorageFreeKb', ))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped, wikiname="DataLoggerReportHrStorageTableRAM")


def main():
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)
    report_ram(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
