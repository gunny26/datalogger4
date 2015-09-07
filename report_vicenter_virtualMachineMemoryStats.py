#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
#import tilak_wiki
from tilak_datalogger import DataLogger as DataLogger
#from tilak_datalogger import DataLoggerHelper as dh
from commons import *

def calc_mem_to_granted_usage(data):
    return data["mem.active.average"] / data["mem.granted.average"]

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa.add_calc_col_full("mem.usage.pct", calc_mem_to_granted_usage)
    tsa.sanitize()
    tsa_grouped = tsa.slice(("mem.active.average",))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped, raw_stat_func="max")

def main():
    project = "vicenter"
    tablename = "virtualMachineMemoryStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    # datestring = "2015-07-03"
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
