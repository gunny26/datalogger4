#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO)
import tilak_wiki
from datalogger import DataLogger as DataLogger
from datalogger import DataLoggerHelper as dh
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa_grouped = tsa.slice(('net.received.average', 'net.transmitted.average'))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "vicenter"
    tablename = "virtualMachineNetworkStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")