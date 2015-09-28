#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLogger as DataLogger
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    # sanitize data
    tsa.sanitize()
    tsa_grouped = tsa.slice(("cpu.ready.summation", "cpu.used.summation"))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    # datastring = "2015-07-01"
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
