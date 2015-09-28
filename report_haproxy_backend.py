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
    tsa.add_per_s_col('bin', 'bin_s')
    tsa.add_per_s_col('bout', 'bout_s')
    tsa.remove_col('bin')
    tsa.remove_col('bout')
    tsa_grouped = tsa.slice(("bin_s", "bout_s"))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "haproxy"
    tablename = "backend"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
