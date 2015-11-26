#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLoggerWeb as DataLoggerWeb
from commons import *

if __name__ == "__main__":
    project = "haproxy"
    tablename = "server"
    datalogger = DataLoggerWeb(DATALOGGER_URL)
    wikiname = datalogger.get_wikiname(project, tablename)
    datestring = datalogger.get_last_business_day_datestring()
    tsastat = datalogger.get_tsastats(project, tablename, datestring)
    tsastat_grouped = tsastat.slice(("bin", "bout"))
    standard_wiki_report(project, tablename, datestring, tsastat, tsastat_grouped, wikiname)
