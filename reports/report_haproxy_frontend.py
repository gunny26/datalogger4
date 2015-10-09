#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import tilak_wiki
from datalogger import DataLogger as DataLogger
from commons import *


def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa.convert('req_tot', "persecond")
    tsa.convert('bin', "counterreset")
    tsa.convert('bout', "counterreset")
    tsa_grouped = tsa.slice(("req_tot", ))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "haproxy"
    tablename = "frontend"
    datalogger = DataLogger(BASEDIR, project, tablename)
    #datestring = "2015-06-29"
    #datestring = get_yesterday_datestring()
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
