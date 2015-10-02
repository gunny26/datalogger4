#!/usr/bin/python
import time
import datetime
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
#import tilak_wiki
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import DataLogger as DataLogger
#from tilak_datalogger import DataLoggerHelper as dh
#from commons import *

BASEDIR = "/var/rrd"
DAY = 60 * 60 *24

def daywalker(days):
    start_ts = time.time() - days * DAY
    while start_ts < time.time():
        yield datetime.date.fromtimestamp(start_ts).isoformat()
        start_ts += DAY

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    try:
        tsa = datalogger.load_tsa(datestring)
        tsa_stat = TimeseriesArrayStats(tsa)
        for key, ts_stat in tsa_stat.stats.items():
            print datestring + "\t" + "\t".join(("%0.2f" % value for value in (ts_stat.stats["pulses"]["min"], ts_stat.stats["pulses"]["avg"], ts_stat.stats["pulses"]["max"], ts_stat.stats["pulses"]["sum"], ts_stat.stats["pulses"]["std"], ts_stat.stats["pulses"]["median"])))
    except StandardError as exc:
        logging.exception(exc)

def main():
    project = "energy"
    tablename = "em1010"
    datalogger = DataLogger(BASEDIR, project, tablename)
    for datestring in daywalker(150):
        report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
