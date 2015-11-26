#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLoggerWeb as DataLoggerWeb
from commons import *

def report(project, tablename, datestring):
    tsa = datalogger.get_tsa(project, tablename, datestring)
    tsa_grouped = tsa.slice(("bin", "bout"))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname)

if __name__ == "__main__":
    project = "haproxy"
    tablename = "backend"
    datalogger = DataLoggerWeb(DATALOGGER_URL)
    wikiname = datalogger.get_wikiname(project, tablename)
    datestring = datalogger.get_last_business_day_datestring()
    report(project, tablename, datestring)
    #cProfile.run("main()")
