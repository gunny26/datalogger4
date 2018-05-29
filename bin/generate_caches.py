#!/usr/bin/python3
import sys
import datetime
import json
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s : %(message)s")
import argparse
# own modules
from datalogger3 import DataLogger as DataLogger
from datalogger3 import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger3 import TimeseriesArray as TimeseriesArray
from datalogger3 import QuantileArray as QuantileArray
from datalogger3 import DataLoggerRawFileMissing as DataLoggerRawFileMissing
from FastTsa import fast_tsa

def gen_caches(project, tablename, datestring, force):
    dl = DataLogger(basedir)
    dl.setup(project, tablename, datestring)
    if force:
        logging.info("deleting pre-created caches (-f was used) and creating TSA from scratch")
        dl.delete_caches()
        dl = DataLogger(basedir)
        dl.setup(project, tablename, datestring)
        logging.info("calling fast_tsa()")
        fast_tsa(dl)
    logging.info("getting caches")
    caches = dl["caches"]
    assert isinstance(caches, dict)
    try:
        if not caches["tsa"]["keys"]:
            # there seems to be no caches at all
            # generate them all, higher memory consumption
            logging.info("no existing TSA and TS Files found, creating from scratch")
            logging.info("calling fast_tsa()")
            fast_tsa(dl)
            #logging.info("archiving to archivepath")
            #dl.raw_to_archive()
        logging.info("reading tsa - or recreating if not present")
        tsa = dl["tsa"] # if tsa exists already only loads structure
        tsa.cache = True
        assert isinstance(tsa, TimeseriesArray)
        logging.info("reading tsastats - or recreating if not present")
        tsastats = dl["tsastats"]
        assert isinstance(tsastats, TimeseriesArrayStats)
        logging.info("reading qa - or recreating if not present")
        qa = dl["qa"]
        assert isinstance(qa, QuantileArray)
        logging.info("reading total_stats - or recreating if not present")
        total_stats = dl["total_stats"]
        assert isinstance(total_stats, dict)
        #logging.info("archiving original input data to archivepath")
        #dl.raw_to_archive()
    except DataLoggerRawFileMissing as exc:
        logging.info("no RAW file avalable")
    dl = DataLogger(basedir)
    dl.setup(project, tablename, datestring)
    caches = dl["caches"]
    num_ts_obj = len(caches["ts"]["keys"].keys())
    logging.info("stats timeseries objects    : %s", num_ts_obj)
    meta = dl.meta
    num_ts = num_ts_obj * len(meta["value_keynames"])
    logging.info("stats individual timeseries : %s", num_ts)
    num_points = 24 * 60 * 60 / meta["interval"] * num_ts
    logging.info("stats individual points     : %s", num_points)

def main():
    datalogger =  DataLogger(basedir)
    for datestring in tuple(datalogger.datewalker(startdate, args.enddate)):
        start_ts, stop_ts = datalogger.get_ts_for_datestring(datestring)
        logging.info("working on datestring %s (from %s to %s)", datestring, start_ts, stop_ts)
        for project in datalogger.get_projects():
            if args.project is not None and project != args.project:
                logging.debug("skipping whole project %s", project)
                continue
            logging.info("working on project %s", project)
            for tablename in datalogger.get_tablenames(project):
                if args.tablename is not None and tablename != args.tablename:
                    logging.debug("skipping %s/%s", project, tablename)
                    continue
                logging.info("working on %s/%s/%s", project, tablename, datestring)
                gen_caches(project, tablename, datestring, args.force)

if __name__ == "__main__":
    basedir = "/var/rrd"
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    parser = argparse.ArgumentParser(description='generate TimeseriesArrays on local backend')
    parser.add_argument('--basedir', default="/var/rrd", help="basedirectory of datalogger data on local machine, default : %(default)s")
    parser.add_argument("-b", '--back', help="how many days back from now")
    parser.add_argument("-s", '--startdate', help="start date in isoformat YYYY-MM-DD")
    parser.add_argument("-e", '--enddate', default=yesterday_datestring, help="stop date in isoformat YYYY-MM-DD, default : %(default)s")
    parser.add_argument("-q", '--quiet', action='store_true', help="set to loglevel ERROR")
    parser.add_argument("-v", '--verbose', action='store_true', help="set to loglevel DEBUG")
    parser.add_argument("-p", '--project', help="process only this project name")
    parser.add_argument("-t", '--tablename', help="process only this tablename")
    parser.add_argument("-f", '--force', action="store_true", help="force recreation of caches")
    args = parser.parse_args()
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    logging.debug(args)
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
    main()
