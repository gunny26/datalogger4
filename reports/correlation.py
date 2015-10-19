#!/usr/bin/pypy
import cProfile
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLogger as DataLogger
#from commons import *

def report(datalogger, datestring):
    cma3 = datalogger.load_correlationmatrix(datestring)
    matrix = cma3["hrStorageUsed"]
    for key in matrix.keys():
        print str(key) + "\t" + "\t".join(("%0.2f" % matrix[key, otherkey] for otherkey in matrix.keys()))

def main():
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger("/var/rrd", project, tablename)
    datestring = datalogger.get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
