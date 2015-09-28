#!/usr/bin/python
import json
import urllib
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLogger as DataLogger
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa = tsa.get_group_by_tsa(("hostname", "vrsrdVirResourceID"), group_func=lambda a: sum(a))
    tsa.add_per_s_col('vrsrdKBRead64', 'vrsrdKBRead64_s')
    tsa.add_per_s_col('vrsrdKBWritten64', 'vrsrdKBWritten64_s')
    tsa.add_per_s_col('vrsrdSCSIReadCmd', 'vrsrdSCSIReadCmd_s')
    tsa.add_per_s_col('vrsrdSCSIWriteCmd', 'vrsrdSCSIWriteCmd_s')
    tsa.remove_col('vrsrdKBRead64')
    tsa.remove_col('vrsrdKBWritten64')
    tsa.remove_col('vrsrdSCSIReadCmd')
    tsa.remove_col('vrsrdSCSIWriteCmd')
    tsa.sanitize()
    #tsa.get_group_by_tsa((subkey,), group_func=lambda a: sum(a))
    tsa_grouped = tsa.slice(('vrsrdSCSIReadCmd_s', 'vrsrdSCSIWriteCmd_s'))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "ipstor"
    tablename = "vrsRealDevTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
