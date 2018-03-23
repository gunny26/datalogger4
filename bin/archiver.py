#!/usr/bin/python
from __future__ import print_function
import cProfile
import pstats
import sys
import gc
import datetime
import shutil
import logging
logging.basicConfig(level=logging.INFO)
import os
import argparse
# own modules
from datalogger import DataLogger as DataLogger

def archive(project, tablename, datestring):
    datalogger = DataLogger(basedir, project, tablename)
    caches = datalogger.get_caches(datestring)
    suffix = "%s/%s/%s\t" % (datestring, project, tablename)
    if caches["tsa"]["raw"] is None:
        logging.debug("%s RAW Data not found", suffix)
    else:
        if not os.path.isfile(caches["tsa"]["raw"]):
            logging.info("%s RAW does not exists, maybe archived or deleted", suffix)
            return
        logging.info("%s found raw file %s", suffix, caches["tsa"]["raw"])
        filebasename = os.path.basename(caches["tsa"]["raw"])
        parts= filebasename.split("_")
        filetablename = filebasename.replace("_%s" % parts[-1], "")
        filedatestring = parts[-1].split(".")[0]
        filesuffix = ".".join(parts[-1].split(".")[1:])
        logging.info("found tablename %s, datestring %s, ending %s", filetablename, filedatestring, filesuffix)
        if (filetablename != tablename) or (filedatestring != datestring):
            logging.error("the references raw file seems not to be the correct one")
        else:
            if filesuffix == "csv.gz":
                logging.info("raw file already zipped, this seems not to be the actual one")
                if (len(caches["tsa"]["keys"]) > 0) and (len(caches["tsastat"]["keys"]) > 0) and (len(caches["ts"]["keys"]) > 0) and (caches["quantile"]["exists"] is True):
                    logging.info("%s all generated archives found, raw data could be archived", suffix)
                    archivepath = os.path.join(args.archivedir, datestring, project, tablename)
                    archivefilename = os.path.join(archivepath, os.path.basename(caches["tsa"]["raw"]))
                    if not os.path.isdir(archivepath):
                        logging.info("creating directory %s", archivepath)
                        os.makedirs(archivepath)
                    logging.info("%s moving raw file to %s", suffix, archivefilename)
                    shutil.move(caches["tsa"]["raw"], archivefilename)
                else:
                    logging.info("%s not all archives available, generate them first, before archiving raw data", suffix)
    del caches
    del datalogger

def main():
    for datestring in tuple(DataLogger.datewalker(startdate, args.enddate)):
        start_ts, stop_ts = DataLogger.get_ts_for_datestring(datestring)
        logging.debug("working on datestring %s (from %s to %s)", datestring, start_ts, stop_ts)
        for project in DataLogger.get_projects(args.basedir):
            if args.project is not None:
                if project != args.project:
                    logging.debug("skipping project %s", project)
                    continue
            logging.debug("working on project %s", project)
            for tablename in DataLogger.get_tablenames(args.basedir, project):
                if args.tablename is not None:
                    if tablename != args.tablename:
                        logging.debug("skipping tablename %s", tablename)
                        continue
                    logging.debug("working on tablename %s", tablename)
                archive(project, tablename, datestring)

if __name__ == "__main__":
    basedir = "/var/rrd"
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    parser = argparse.ArgumentParser(description='generate TimeseriesArrays on local backend')
    parser.add_argument('--basedir', default="/var/rrd", help="basedirectory of datalogger data on local machine (default: %(default)s)")
    parser.add_argument("-s", '--startdate', help="start date in isoformat YYYY-MM-DD")
    parser.add_argument("-e", '--enddate', default=yesterday_datestring, help="stop date in isoformat YYYY-MM-DD")
    parser.add_argument("-q", '--quiet', action='store_true', help="set to loglevel ERROR")
    parser.add_argument("-v", '--verbose', action='store_true', help="set to loglevel DEBUG")
    parser.add_argument("-p", '--project', help="process only this project name")
    parser.add_argument("-t", '--tablename', help="process only this tablename")
    parser.add_argument("-a", '--archivedir', default="/srv/raw-archiv/datalogger_raw_archiv/", help="directory to archive old raw data to (default: %(default)s)")
    parser.add_argument("--profile", action="store_true", help="use cProfile to start main")
    args = parser.parse_args()
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    logging.debug(args)
    startdate = None
    if args.startdate is not None:
        startdate = args.startdate
    else:
        logging.error("you have to provide -s")
        sys.exit(1)
    if args.profile is True:
        logging.info("profiling enabled")
        pstatfilename = "profile.stat"
        cProfile.run('main()', pstatfilename)
        stats = pstats.Stats(pstatfilename)
        stats.strip_dirs()
        stats.sort_stats("cumulative")
        stats.print_stats()
        logging.info("INCOMING CALLERS")
        stats.print_callers()
        logging.info("OUTGOING CALLEES")
        stats.print_callees()
    else:
        main()
