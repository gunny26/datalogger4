#!/usr/bin/pypy
import cProfile
import time
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import datalogger
from datalogger import DataLogger as DataLogger
from datalogger import QuantillesArray as QuantillesArray
from commons import *

def report(datalogger, datestring):
    qa = datalogger.load_quantilles(datestring)
    print "Output"
    print qa[("srvarthur1.tilak.cc","0")]
    #quantilles = Quantilles(tsa, "cpu.used.summation", maxx=None)
    quantilles = qa["cpu.used.summation"]
    #quantilles = Quantilles(tsa, "datastore.read.average", maxx=None)
    quantilles.sort(2)
    print "most demanding CPU Cores"
    print quantilles.head(20)
    print "least demanding CPU Cores"
    print quantilles.tail(20)

def main():
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
