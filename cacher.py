#!/usr/bin/python
import cProfile
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import multiprocessing
#import Queue
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
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


def report(datalogger, datestring):
    tsastats = datalogger.load_tsastats(datestring)
    key = tsastats.keys()[0]
    value_key = tsastats[key].keys()[0]
    funcname = tsastats[key].funcnames[0]
    print datestring, key, value_key, funcname, tsastats[key][value_key][funcname]
    return


def worker():
    while not queue.empty():
        (project, tablename, datestring) = queue.get()
        logging.info("working on %s, %s, %s", datestring, project, tablename)
        datalogger = DataLogger(BASEDIR, project, tablename)
        caches = datalogger.get_caches(datestring)
        if caches["tsa"]["raw"] is None:
            logging.info("Skipping, no input file available")
        try:
            datalogger[datestring]
            #report(datalogger, datestring)
        except StandardError as exc:
            logging.error("Error on %s, %s, %s", datestring, project, tablename)
        #queue.task_done()

queue = multiprocessing.Queue()
if __name__ == "__main__":
    for project in DataLogger.get_projects(BASEDIR):
        for tablename in DataLogger.get_tablenames(BASEDIR, project):
            datalogger = DataLogger(BASEDIR, project, tablename)
            for datestring in datewalker("2015-09-01", "2015-10-01"):
                queue.put((project, tablename, datestring))
    for threadid in range(16):
        t = multiprocessing.Process(target=worker)
        #t.daemon = True
        t.start()
    queue.join()
    logging.info("Ending")
