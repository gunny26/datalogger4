#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLogger as DataLogger
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa = tsa.slice((u"fcIfC3Discards", u'fcIfC3InOctets', u'fcIfC3OutOctets'))
    tsa.convert('fcIfC3InOctets', "persecond")
    tsa.convert('fcIfC3OutOctets', "persecond")
    # for grouped reports, reduce number of cols
    tsa_grouped = tsa.slice((u'fcIfC3InOctets', u'fcIfC3OutOctets'))
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
