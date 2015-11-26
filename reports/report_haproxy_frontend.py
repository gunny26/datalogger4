#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import tilak_wiki
from datalogger import DataLoggerWeb as DataLoggerWeb
from commons import *

if __name__ == "__main__":
    project = "haproxy"
    tablename = "frontend"
    datalogger = DataLoggerWeb(DATALOGGER_URL)
    wikiname = datalogger.get_wikiname(project, tablename)
    datestring = datalogger.get_last_business_day_datestring()
    tsa = datalogger.get_tsa(project, tablename, datestring)
    tsa_grouped = tsa.slice(("req_tot", ))
    standard_wiki_report(project, tablename, datestring, tsa, tsa_grouped, wikiname=wikiname)
