#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO)
from tilak_datalogger import DataLogger as DataLogger
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.read_day(datestring)
    tsa.add_derive_col('fcIfC3InOctets', 'fcIfC3InOctets_d')
    tsa.add_derive_col('fcIfC3OutOctets', 'fcIfC3OutOctets_d')
    tsa.remove_col('fcIfC3InOctets')
    tsa.remove_col('fcIfC3OutOctets')
    # get rid of unusable data
    tsa.sanitize()
    # for grouped reports, reduce number of cols
    tsa_grouped = tsa.slice(('fcIfC3InOctets_d', 'fcIfC3OutOctets_d'))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "sanportperf"
    tablename = "fcIfC3AccountingTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
