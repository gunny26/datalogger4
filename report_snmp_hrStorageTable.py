#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO)
from tilak_datalogger import DataLogger as DataLogger
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.read_tsa_full(datestring)
    # get rid of unusable data
    tsa.sanitize()
    # for grouped reports, reduce number of cols
    tsa_grouped = tsa.slice(('hrStorageUsed', ))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    datestring = "2015-07-28"
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
