#!/usr/bin/python
from __future__ import print_function
import cProfile
import sys
import gc
import datetime
import logging
logging.basicConfig(level=logging.DEBUG)
import argparse
# own modules
from datalogger import DataLogger as DataLogger

def main(project, tablename, datestring):
    datalogger = DataLogger(basedir, project, tablename)
    caches = datalogger.get_caches(datestring)
    suffix = "%s/%s/%s\t" % (datestring, project, tablename)
    data = None
    if caches["tsa"]["raw"] is None:
        print(suffix, "Nothing could be done without RAW data")
    else:
        if len(caches["tsa"]["keys"]) == 0:
            print(suffix, "TSA Archive missing, calling get_tsa and get_tsastats")
            data = datalogger.load_tsa(datestring)
        else:
            if len(caches["tsastat"]["keys"]) == 0:
                print(suffix, "TSASTAT Archive missing, calling get_tsastats")
                data = datalogger.load_tsastats(datestring)
            else:
                if len(caches["ts"]["keys"]) == 0:
                    print(suffix, "there are no ts archives, something went wrong, or tsa is completely empty, calling get_tsastats")
                    data = datalogger.load_tsa(datestring)
                else:
                    print(suffix, "All fine")
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
    args = parser.parse_args()
    print(args)
    if (args.back is not None) == (args.startdate is not None):
        print("option -b and -e are mutual exclusive, use only one")
        sys.exit(1)
    startdate = None
    if args.back is not None:
        startdate = (datetime.date.today() - datetime.timedelta(int(args.back))).isoformat()
    elif args.startdate is not None:
        startdate = args.startdate
    else:
        print("you have to provide either -b or -s")
        sys.exit(1)
    for datestring in tuple(DataLogger.datewalker(startdate, args.enddate)):
        for project in DataLogger.get_projects(args.basedir):
            for tablename in DataLogger.get_tablenames(args.basedir, project):
                main(project, tablename, datestring)
    #cProfile.run("main()")
