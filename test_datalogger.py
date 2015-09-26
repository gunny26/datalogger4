#!/usr/bin/python
import cProfile
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import calendar
import tilak_datalogger
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import TimeseriesArray as TimeseriesArray
from tilak_datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from commons import *

def datestring_to_date(datestring):
    year, month, day = datestring.split("-")
    return datetime.date(int(year), int(month), int(day))

def datewalker(datestring_start, datestring_stop):
    start_date = datestring_to_date(datestring_start)
    stop_date = datestring_to_date(datestring_stop)
    while start_date <= stop_date:
        yield start_date.isoformat()
        start_date = start_date + datetime.timedelta(days=1)

def monthwalker(monthdatestring):
    year, month = monthdatestring.split("-")
    firstweekday, last = calendar.monthrange(int(year), int(month))
    start = "%04d-%02d-01" % (int(year), int(month))
    stop = "%04d-%02d-%02d" % (int(year), int(month), last)
    return datewalker(start, stop)


def table_by_index(tsa_stat, key):
    ts_stat = tsa_stat[key]
    funcnames = sorted(ts_stat.funcnames)
    outbuffer = []
    outbuffer.append("| value_keyname | " + " | ".join(funcnames) + " |")
    for value_key in ts_stat.keys():
        linebuffer = []
        linebuffer.append(value_key)
        # keep funcnames in order with headers
        for value in (ts_stat[value_key][funcname] for funcname in funcnames):
            #print function, value
            linebuffer.append("%0.2f" % value)
        #outbuffer.append("| " + function + " | " + ts_stat[function] + " |")
        #print "| " + " | ".join(linebuffer) + " |"
        outbuffer.append("| " + " | ".join(linebuffer) + " |")
    print "\n".join(outbuffer)

def main():
    project = "nagios"
    for tablename in DataLogger.get_tablenames(BASEDIR, project):
        datalogger = DataLogger(BASEDIR, project, tablename)
        for datestring in datewalker("2015-04-01", "2015-09-23"):
            print datestring, tablename
            try:
                caches = datalogger.get_caches(datestring)
                #for cachetype, cachedata in caches.items():
                #    print "Caches for %s" % cachetype
                #    for key, filename in cachedata["keys"].items():
                #        print "\tfound %s in\n\t\t%s" % (key, filename)
                # there should be only one tsa file
                #print "Number of caches TimeseriesArray objects:", len(caches["tsa"]["keys"])
                #print "Number of caches TimeseriesArrayStats objects:", len(caches["tsastat"]["keys"])
                #print "Number of caches Timeseries objects:", len(caches["ts"]["keys"])
                #print "Number of caches TimeseriesStats objects:", len(caches["tsstat"]["keys"])
                if len(caches["tsa"]["keys"]) == 0:
                    print datestring, "TimeseriesArray cache missing"
                    datalogger.load_tsa(datestring)
                else:
                    #datalogger[datestring] # read from raw, and store tsa and ts caches
                    if len(caches["tsa"]["keys"]) != len(caches["tsastat"]["keys"]):
                        print datestring, "TimeseriesArrayStats caches missing"
                        datalogger.load_tsastats(datestring)
                    else:
                        if len(caches["ts"]["keys"]) != len(caches["tsstat"]["keys"]):
                            print datestring, "Number ob Timeseries and TimeseriesStats should be the same"
                        if len(caches["ts"]["keys"]) > len(caches["tsstat"]["keys"]):
                            print datestring, "some missing TimeseriesStats"
            except tilak_datalogger.DataLoggerRawFileMissing as exc:
                #logging.exception(exc)
                logging.info("%s no RAW Data available", datestring)
                pass
            except StandardError as exc:
                logging.exception(exc)
                pass
                #print datestring, exc


if __name__ == "__main__":
    main()
    # cProfile.run("main()")
