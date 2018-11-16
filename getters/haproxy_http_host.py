#!/usr/bin/python
"""
Program to read and parse haproxylogs to put them in shape to upload to DataLogger
The input date schould be sorted by date, and finished
the uploaded data will immediately split into TimeseriesArray, so no further data
of this day could be appended
"""
import os
import sys
import datetime
import gzip
import argparse
import logging
logging.basicConfig(level=logging.INFO)
# own modules
from tk_webapis import HaproxyLogWebClient as HaproxyLogWebClient

def datestring_to_date(datestring):
    """
    convert string in format YYYY-MM-DD into date object
    """
    year, month, day = datestring.split("-")
    date = datetime.date(year=int(year), month=int(month), day=int(day))
    return date

def datewalk(datestring1, datestring2):
    """
    count up from datestring1 to datestring2 in single day steps
    yield in isoformat()
    """
    date1 = datestring_to_date(datestring1)
    date2 = datestring_to_date(datestring2)
    assert date2 > date1
    oneday = datetime.timedelta(1)
    while date1 <= date2:
        yield date1.isoformat()
        date1 += oneday

def main():
    """
    what do you think, what main should do
    """
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    parser = argparse.ArgumentParser(description='generate TimeseriesArrays on local backend')
    parser.add_argument("-b", '--back', help="how many days back from now")
    parser.add_argument("-s", '--startdate', help="start date in isoformat YYY-MM-DD")
    parser.add_argument("-e", '--enddate', default=yesterday_datestring, help="stop date in isoformat YYY-MM-DD")
    parser.add_argument("-q", '--quiet', action='store_true', help="set to loglevel ERROR")
    parser.add_argument("-v", '--verbose', action='store_true', help="set to loglevel DEBUG")
    args = parser.parse_args("-b 2".split())
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
    # lets get started
    project = "haproxy"
    tablename = "http_host"
    # filename /var/rrd/haproxy/raw/http_host_2018-11-14.csv.gz
    basedir = "/var/rrd"
    logging.info("getting logs in between of %s and %s", startdate, args.enddate)
    for datestring in datewalk(startdate, args.enddate):
        filename = os.path.join(basedir, project, "raw", "%s_%s.csv.gz" % (tablename, datestring))
        if os.path.isfile(filename):
            logging.info("skipping this datestring, raw data %s already available" % filename)
            continue
        try:
            logging.info("getting data for %s and storing in %s" % (datestring, filename))
            with gzip.open(filename, "wt") as outfile:
                hlwc = HaproxyLogWebClient()
                for line in hlwc.datalogger(datestring):
                    outfile.write(line + "\n")
                os.fchown(outfile, pwd.getpwnam("www-data").pw_uid, pwd.getpwnam("www-data").pw_gid)
        except IOError as exc:
            logging.error(exc)
            logging.error("Exception on file datestring %s, skipping this date", datestring)
    logging.info("--- done ---")

if __name__ == "__main__":
    main()
 
