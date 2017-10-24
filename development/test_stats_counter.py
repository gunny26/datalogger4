#!/usr/bin/python
from __future__ import print_function
import cProfile
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import calendar
import json
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import TimeseriesStats as TimeseriesStats
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

def month_report_hc(project, tablename, key, value_key):
    datalogger = DataLogger(BASEDIR, project, tablename)
    filterkeys = dict(zip(datalogger.index_keynames, key))
    print(filterkeys)
    ret_data = {}
    for datestring in monthwalker("2017-09"):
        print("getting tsatstats for ", datestring)
        try:
            tsa = datalogger[(datestring, key)]
            for key in tsa.keys():
                print(key)
            ts = datalogger[(datestring, key, value_key)]
            print("\t%d" % max(ts))
            tsastats = datalogger.load_tsastats(datestring, filterkeys=filterkeys)
            print("\t%d" % tsastats[key][value_key]["max"])
        except Exception as exc:
            print(exc)
            break
        for funcname in tsastats[key][value_key].keys():
            if funcname in ret_data:
                ret_data[funcname].append((datestring, tsastats[key][value_key][funcname]))
            else:
                ret_data[funcname] = [(datestring, tsastats[key][value_key][funcname]), ]
    for row in [{ "name" : funcname, "data" : ret_data[funcname]} for funcname in ret_data.keys()]:
        for key, value in row.items():
            print(key, value)


def main():
    datestring = "2017-09-19"
    project = "sanportperf"
    tablename = "fcIfC3AccountingTable"
    key = (u"fca-sr1-8gb-11", u"port-channel 1")
    value_key = "fcIfC3InOctets"
    datalogger = DataLogger(BASEDIR, project, tablename)
    # get TimeseriesArray of one day
    tsa = datalogger["2017-09-19"]
    tsa = datalogger.load_tsa("2017-09-19")
    # show all available index_keys in this Array
    print(tsa.keys())
    # get Timeseries for one index_key
    ts = tsa[key]
    # get timeseries for one value_key
    print(ts[value_key])
    print(max(ts[value_key]))
    tsstats = TimeseriesStats(ts)
    print("MAX calucluated : %s" % tsstats[value_key]["max"])
    tsastats = TimeseriesArrayStats(tsa)
    print("MAX calculated __getitem__ : %d" % tsastats[key][value_key]["max"])
    tsastats_2 = datalogger.load_tsastats("2017-09-19")
    print("MAX loaded via datalogger : %d" % tsastats_2[key][value_key]["max"])
    dumpfile = tsastats.get_tsstat_dumpfilename(key)
    raw_data = json.load(open("/var/rrd/global_cache/%s/%s/%s/%s" % (datestring, project, tablename, dumpfile)))
    print(json.dumps(raw_data, indent=4))
    print("MAX from stored json file : %d" % raw_data[value_key]["max"])
    tsastats.dump("/tmp/")
    raw_data = json.load(open("/tmp/%s" % dumpfile))
    print(json.dumps(raw_data, indent=4))
    # month_report_hc(project, tablename, key, value_key)

if __name__ == "__main__":
    #main()
    cProfile.run("main()")
