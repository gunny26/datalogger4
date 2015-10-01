#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa.add_per_s_col('fcIfC3InOctets', 'fcIfC3InOctets_s')
    tsa.add_per_s_col('fcIfC3OutOctets', 'fcIfC3OutOctets_s')
    tsa.add_per_s_col('fcIfC3InFrames', 'fcIfC3InFrames_s')
    tsa.add_per_s_col('fcIfC3OutFrames', 'fcIfC3OutFrames_s')
    tsa.remove_col('index')
    tsa.remove_col('fcIfC3InOctets')
    tsa.remove_col('fcIfC3OutOctets')
    tsa.remove_col('fcIfC3InFrames')
    tsa.remove_col('fcIfC3OutFrames')
    # for grouped reports, reduce number of cols
    tsa_grouped = tsa.slice(('fcIfC3InOctets_s', 'fcIfC3OutOctets_s'))
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
