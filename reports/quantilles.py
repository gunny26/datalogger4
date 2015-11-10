#!/usr/bin/pypy
import cProfile
import time
import sys
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import datalogger
from datalogger import DataLoggerWeb as DataLoggerWeb
#from datalogger import QuantillesArray as QuantillesArray
#from commons import *

BASEDIR = "/var/rrd"
DATALOGGER_URL = "http://srvmgdata1.tilak.cc/DataLogger"

def report(project, tablename, datestring):
    qa = datalogger.get_quantilles(project, tablename, datestring)
    quantilles = qa["cpu.used.summation"]
    #quantilles.sort(2)
    print "most demanding CPU Cores"
    print quantilles.head(20)
    print "least demanding CPU Cores"
    print quantilles.tail(20)

def main():
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datestring = datalogger.get_last_business_day_datestring()
    #datestring = DataLogger.get_last_business_day_datestring()
    if len(sys.argv) == 2:
        datestring = sys.argv[1]
    if len(sys.argv) == 2:
        datestring = sys.argv[1]
    #datalogger = DataLogger(BASEDIR, project, tablename)
    report(project, tablename, datestring)

if __name__ == "__main__":
    datalogger = DataLoggerWeb(DATALOGGER_URL)
    main()
    #cProfile.run("main()")
