#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import tilak_wiki
from datalogger import DataLoggerWeb as DataLoggerWeb
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from commons import *

def report_haproxy_backend(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("bin", "bout", ))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_haproxy_frontend(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("req_tot", ))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_haproxy_server(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("bin", "bout"))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_ipstor_vrsClientTable(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(('vrsclKBRead64', 'vrsclKBWritten64'))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_ipstor_vrsRealDevTable(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(('vrsrdSCSIReadCmd', 'vrsrdSCSIWriteCmd'))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_sanportperf_fcIfC3AccountingTable(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(('fcIfC3InOctets', 'fcIfC3OutOctets'))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_snmp_hrStorageTable(project, tablename, wikiname):
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
    tsa = datalogger.get_tsa(project, tablename, datestring)
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
    standard_wiki_report(project, tablename, datestring, TimeseriesArrayStats(tsa), TimeseriesArrayStats(tsa_grouped), wikiname=wikiname)
    # only RAM
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.get_tsa(project, tablename, datestring)
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
    standard_wiki_report(project, tablename, datestring, TimeseriesArrayStats(tsa), TimeseriesArrayStats(tsa_grouped), wikiname="DataLoggerReportHrStorageTableRAM")

def report_vicenter_hostSystemCpuStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(('cpu.used.summation', ))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_vicenter_hostSystemDiskStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("disk.deviceWriteLatency.average", "disk.deviceReadLatency.average"))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_vicenter_hostSystemNetworkStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice((u'net.received.average', u'net.transmitted.average'))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_vicenter_virtualMachineCpuStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("cpu.ready.summation", "cpu.used.summation"))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_vicenter_virtualMachineDatastoreStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("datastore.read.average", "datastore.write.average", "datastore.totalReadLatency.average", "datastore.totalWriteLatency.average"))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_vicenter_virtualMachineDiskStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("disk.read.average", "disk.write.average", 'disk.commands.summation'))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_vicenter_virtualMachineMemoryStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(("mem.active.average",))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

def report_vicenter_virtualMachineNetworkStats(project, tablename, wikiname):
    tsa = datalogger.get_tsastats(project, tablename, datestring)
    tsa_grouped = tsa.slice(('net.received.average', 'net.transmitted.average'))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)

if __name__ == "__main__":
    datalogger = DataLoggerWeb(DATALOGGER_URL)
    datestring = datalogger.get_last_business_day_datestring()
    for project in datalogger.get_projects():
        for tablename in datalogger.get_tablenames(project):
            funcname = "report_%s_%s" % (project, tablename)
            try:
                func = eval(funcname)
                logging.info("calling %s()", funcname)
                func(project, tablename, datalogger.get_wikiname(project, tablename))
            except NameError:
                logging.error("no reporting function named %s defined", funcname)
