#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from tilak_datalogger import DataLogger as DataLogger
from commons import *

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.read_day(datestring)
    tsa.sanitize()
    tsa_grouped = tsa.slice(("disk.read.average", "disk.write.average", 'disk.commands.summation'))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "vicenter"
    tablename = "virtualMachineDiskStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
