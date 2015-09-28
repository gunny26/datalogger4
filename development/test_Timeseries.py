#!/usr/bin/python
import cProfile
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import calendar
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
    project = "snmp"
    tablename = "hrStorageTable"
    datalogger = DataLogger(BASEDIR, project, tablename)
    key = ("srvcacdbp1.tilak.cc","E:\\ Label:New Volume  Serial Number f2423c17","HOST-RESOURCES-TYPES::hrStorageFixedDisk")
    value_key = "hrStorageUsed"
    print "get tsa for one whole day"
    assert type(datalogger["2015-09-16"]) == TimeseriesArray
    #tsa = datalogger.load_tsa("2015-09-16", filterkeys=dict(zip(datalogger.index_keynames, (u'srvazwra02.tilak.cc', None, None))))
    #print tsa.keys()
    print "get one tsa for one day, with multiple timeseries"
    assert len(datalogger["2015-09-16", (u'srvazwra02.tilak.cc', None, None)].keys()) > 1
    print "get timeseries data"
    print datalogger["2015-09-16", (u'srvazwra02.tilak.cc', u'Cached memory', u'HOST-RESOURCES-TYPES::hrStorageOther')]
    print datalogger["2015-09-16", (u'srvazwra02.tilak.cc', u'Cached memory', u'HOST-RESOURCES-TYPES::hrStorageOther'), 0]
    print datalogger["2015-09-16", (u'srvazwra02.tilak.cc', u'Cached memory', u'HOST-RESOURCES-TYPES::hrStorageOther'), 0, "hrStorageUsed"]
    print datalogger["2015-09-16", (u'srvazwra02.tilak.cc', u'Cached memory', u'HOST-RESOURCES-TYPES::hrStorageOther'), None, "ts"]
    print datalogger["2015-09-16", (u'srvazwra02.tilak.cc', u'Cached memory', u'HOST-RESOURCES-TYPES::hrStorageOther'), 1442403584.0, "hrStorageUsed"]
    print datalogger["2015-09-16", (u'srvazwra02.tilak.cc', u'Cached memory', u'HOST-RESOURCES-TYPES::hrStorageOther'), 1442403584.0]
    #tsa = datalogger.load_tsa("2015-09-16")
    #print tsa.keys()[0]
    #ts = tsa[tsa.keys()[0]]
    ##print tsa[tsa.keys()[0]][1442403584.0, "hrStorageUsed"]
    #print ts.colnames
    #print ts
    #print ts[0]
    #print ts[0, "hrStorageUsed"]
    #print ts[None, "ts"]
    #print ts[1442403584.0, "hrStorageUsed"]
    #print ts[1442403584.0]
    # this shouldnt work
    #print ts[1442403584.1]

if __name__ == "__main__":
    main()
    # cProfile.run("main()")
