#!/usr/bin/python
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import tilak_wiki
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import DataLoggerHelper as dh
from commons import *

def report(datalogger, datestring):
    tsa = datalogger.read_day(datestring)
    # sanitize data
    tsa.sanitize()
    tsa_grouped = tsa.slice(('mem.active.average', ))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    basedir = "/var/rrd/"
    project = "vicenter"
    tablename = "hostSystemMemoryStats"
    datalogger = DataLogger(basedir, project, tablename)
    datestring = get_last_business_day_datestring()
    # datestring = "2015-07-08"
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
