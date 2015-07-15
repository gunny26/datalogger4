#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO)
import tilak_wiki
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import DataLoggerHelper as dh
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.read_day(datestring)
    tsa.sanitize()
    tsa_grouped = tsa.slice(('net.received.average', 'net.transmitted.average'))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    basedir = "/var/rrd/"
    project = "vicenter"
    tablename = "virtualMachineNetworkStats"
    datalogger = DataLogger(basedir, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
