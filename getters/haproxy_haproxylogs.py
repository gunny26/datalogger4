#!/usr/bin/python
"""
Program to read and parse haproxylogs to put them in shape to upload to DataLogger
The input date schould be sorted by date, and finished
the uploaded data will immediately split into TimeseriesArray, so no further data
of this day could be appended
"""
import os
import sys
import gzip
import logging
logging.basicConfig(level=logging.DEBUG)
import datetime
import zlib
import requests
import StringIO
import argparse
# own modules
from datalogger import DataLoggerWeb as DataLoggerWeb
import tilak_haproxylog

def aggregator(index_keynames, value_keynames, ts_keyname, func, interval = 60 * 5):
    """
    aggregates some protocol data to get a consistent timeseries,
    with interval
    """
    data = {}
    ts = None
    #print ts_keyname
    for parsts, parsdata in func():
        #print parsdata
        #print parsdata["log_timestamp"]
        if ts is None:
            ts = parsts
        key = tuple((parsdata[key] for key in index_keynames))
        values = tuple((int(parsdata[key]) for key in value_keynames))
        if key not in data:
            data[key] = values
        else:
            data[key] = tuple((data[key][index] + int(values[index]) for index in range(len(values))))
        if parsts > (ts + interval):
            for keys, values in data.items():
                yield "%s\t%s\t%s" % (ts, "\t".join((str(index_key) for index_key in keys)), "\t".join((str(value_key) for value_key in values)))
            ts = None
            data = {}

def parser_generator(index_keynames, value_keynames, file_obj):
    """
    return specific parser for this set of parameters
    """
    def inner():
        """
        split line into dict of fields,
        and append some data according to line
        """
        for line in file_obj:
            logdata = tilak_haproxylog.parse_line(line)
            if logdata is not None:
                logdata["hits"] = 1
                for value_key in value_keynames:
                    if value_key not in logdata:
                        logdata[value_key] = 0
                status_code = int(logdata["status_code"])
                if 100 <= status_code <= 199:
                    logdata["rsp_1xx"] = 1
                elif 200 <= status_code <= 299:
                    logdata["rsp_2xx"] = 1
                elif 300 <= status_code <= 399:
                    logdata["rsp_3xx"] = 1
                elif 400 <= status_code <= 499:
                    logdata["rsp_4xx"] = 1
                elif 500 <= status_code <= 599:
                    logdata["rsp_5xx"] = 1
                else:
                    logdata["rsp_other"] = 1
                ret_data = dict(zip(index_keynames, (logdata[index_key] for index_key in index_keynames)))
                ret_data.update(dict(zip(value_keynames, (logdata[value_key] for value_key in value_keynames))))
                yield (logdata["ts"], ret_data)
    return inner

def generate_datalogger_csv(logdir, datestring, keys, values, ts_keyname):
    """
    create CSV like file with StringIO
    """
    if datestring == datetime.date.today().isoformat():
        logging.error("todays Logs are actually written and cannot used in datalogger")
        return
    headers = [ts_keyname, ] + list(keys) + list(values)
    linebuffer = []
    linebuffer.append("\t".join(headers)) 
    filename = os.path.join(logdir, "haproxylog_%s.gz" % datestring)
    logging.info("parsing file %s", filename)
    try:
        parser = parser_generator(keys, values, gzip.open(filename, "rb"))
        for line in aggregator(keys, values, ts_keyname, parser):
            linebuffer.append(line)
    except IOError as exc:
        logging.exception(exc)
    return StringIO.StringIO("\n".join(linebuffer))

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
    while date1 < date2:
        yield date1.isoformat()
        date1 += oneday

def main():
    """
    what do you think, what main should do
    """
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    parser = argparse.ArgumentParser(description='generate TimeseriesArrays on local backend')
    parser.add_argument('--url', default="http://srvmgdata1.tilak.cc/DataLogger", help="url of DataLogger Webapplication")
    parser.add_argument('--logdir', default="/data1/haproxy_daily/", help="directory where to find day sorted haproxylogs")
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
    # lets get started
    datalogger = DataLoggerWeb(args.url)
    project = "haproxy"
    tablename = "http_host"
    baseurl = "%s/upload_raw_file/" % args.url
    logdir = args.logdir # where to find haproxy logs
    keys = ("http_host", )
    values = ("bytes_read", "rsp_1xx", "rsp_2xx", "rsp_3xx", "rsp_4xx", "rsp_5xx", "rsp_other", "srv_queue", "backend_queue", "actconn", "feconn", "beconn", "srv_conn", "retries", "tq", "tw", "tc", "tr", "tt", "hits")
    ts_keyname = "ts"
    for datestring in datewalk(startdate, args.enddate):
        caches = datalogger.get_caches(project, tablename, datestring)
        if caches["tsa"]["raw"] is not None:
            logging.info("Skipping this datestring, raw data is already available")
            continue
        try:
            stringio = generate_datalogger_csv(logdir, datestring, keys, values, ts_keyname)
            #upload data
            files = {'myfile': stringio}
            url = "/".join((baseurl, project, tablename, datestring))
            logging.info("calling %s", url)
            response = requests.post(url, files=files)
            print response.content
        except StandardError as exc:
            logging.error("Exception on file datestring %si, skipping this date", datestring)
        except zlib.error as exc:
            logging.error(exc)

if __name__ == "__main__":
    main()
 
