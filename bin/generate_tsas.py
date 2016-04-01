#!/usr/bin/python
from __future__ import print_function
import cProfile
import sys
import gc
import datetime
import logging
logging.basicConfig(level=logging.INFO)
import argparse
# own modules
from datalogger import DataLogger as DataLogger

def main(project, tablename, datestring):
    datalogger = DataLogger(basedir, project, tablename)
    caches = datalogger.get_caches(datestring)
    suffix = "%s/%s/%s\t" % (datestring, project, tablename)
    data = None
    if caches["tsa"]["raw"] is None:
        logging.info("%s Nothing could be done without RAW data", suffix)
    else:
        if len(caches["tsa"]["keys"]) == 0:
            logging.info("%s TSA Archive missing, calling get_tsa and get_tsastats", suffix)
            data = datalogger.load_tsastats(datestring)
        else:
            if len(caches["tsastat"]["keys"]) == 0:
                logging.info("%s TSASTAT Archive missing, calling get_tsastats", suffix)
                data = datalogger.load_tsastats(datestring)
            else:
                if len(caches["ts"]["keys"]) == 0:
                    logging.info("%s there are no ts archives, something went wrong, or tsa is completely empty, calling get_tsastats", suffix)
                    data = datalogger.load_tsastats(datestring)
                else:
                    logging.info("%s All fine", suffix)
    del data
    del caches
    del datalogger
    #print(gc.get_count())

if __name__ == "__main__":
    basedir = "/var/rrd"
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    parser = argparse.ArgumentParser(description='generate TimeseriesArrays on local backend')
    parser.add_argument('--basedir', default="/var/rrd", help="basedirectory of datalogger data on local machine")
    parser.add_argument("-b", '--back', help="how many days back from now")
    parser.add_argument("-s", '--startdate', help="start date in isoformat YYY-MM-DD")
    parser.add_argument("-e", '--enddate', default=yesterday_datestring, help="stop date in isoformat YYY-MM-DD")
    parser.add_argument("-q", '--quiet', action='store_true', help="set to loglevel ERROR")
    parser.add_argument("-v", '--verbose', action='store_true', help="set to loglevel DEBUG")
    args = parser.parse_args()
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    if (args.back is not None) == (args.startdate is not None):
        logging.error("option -b and -e are mutual exclusive, use only one")
        sys.exit(1)
    startdate = None
    if args.back is not None:
        startdate = (datetime.date.today() - datetime.timedelta(int(args.back))).isoformat()
    elif args.startdate is not None:
        startdate = args.startdate
    else:
        logging.error("you have to provide either -b or -s")
        sys.exit(1)
    for datestring in tuple(DataLogger.datewalker(startdate, args.enddate)):
        for project in DataLogger.get_projects(args.basedir):
            for tablename in DataLogger.get_tablenames(args.basedir, project):
                main(project, tablename, datestring)
    #cProfile.run("main()")
