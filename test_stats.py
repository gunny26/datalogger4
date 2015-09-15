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


def report(datalogger, datestring, key, value_key):
    # get data, from datalogger, or dataloggerhelper
    #tsa = datalogger.load_tsa(datestring)
    #tsastats = TimeseriesArrayStats(tsa)
    #tsastats.dump("/tmp/")
    #tsastats2 = TimeseriesArrayStats.load("/tmp", datalogger.index_keynames)
    #assert tsastats == tsastats2
    tsastats = datalogger.load_tsastats(datestring)
    #assert tsastats == tsastats3
    #assert len(tsastats) == len(tsastats2)
    #for key in tsastats.keys():
    #    print "checkinf __eq__ of key %s" % str(key)
    #    assert tsastats[key] == tsastats2[key]
    #key = tsastats.keys()[0]
    #print tsastats[key].keys()
    #value_key = tsastats[key].keys()[0]
    #funcname = tsastats[key].funcnames[0]
    #print datestring, key, value_key, funcname, tsastats[key][value_key][funcname]
    ret_data = tsastats[key][value_key]
    #table_by_index(tsastats, (u'srvhlodgs1.tilak.cc', u'Physical Memory', u'HOST-RESOURCES-TYPES::hrStorageRam'))
    #table_by_index(tsastats2, (u'srvhlodgs1.tilak.cc', u'Physical Memory', u'HOST-RESOURCES-TYPES::hrStorageRam'))
    #table_by_index(tsastats3, (u'srvhlodgs1.tilak.cc', u'Physical Memory', u'HOST-RESOURCES-TYPES::hrStorageRam'))
    #for key, ts_stats in tsa_stat.stats.items():
    #    if u"HOST-RESOURCES-TYPES::hrStorageRam" in key:
    #        print key, ts_stats.stats['hrStorageUsed']["std"]
    return ret_data


def month_report(project, tablename, key, value_key):
    datalogger = DataLogger(BASEDIR, project, tablename)
    filterkeys = dict(zip(datalogger.index_keynames, key))
    print filterkeys
    ret_data = {}
    for datestring in monthwalker("2015-08"):
        print "getting tsatstats for ", datestring
        tsastats = datalogger.load_tsastats(datestring, filterkeys=filterkeys)
        ret_data[datestring] = tsastats[key][value_key]
        #data["key"] = key
        #data["datestring"] = datestring
        #data["project"] = datalogger.project
        #data["tablename"] = datalogger.tablename
        #print data
        #ret_data.append(data)
    headers = ["datestring", ]
    colkeys = ret_data[ret_data.keys()[0]].keys()
    headers += colkeys
    print "| *" + "* | *".join(headers) + "* |"
    for datestring in sorted(ret_data.keys()):
        datevalues = ret_data[datestring]
        print "| " + datestring + " | " + " | ".join((str(datevalues[key]) for key in colkeys)) + " |"
    #report_ram(datalogger, datestring)

def month_report_hc(project, tablename, key, value_key):
    datalogger = DataLogger(BASEDIR, project, tablename)
    filterkeys = dict(zip(datalogger.index_keynames, key))
    print filterkeys
    ret_data = {}
    for datestring in monthwalker("2015-08"):
        print "getting tsatstats for ", datestring
        tsastats = datalogger.load_tsastats(datestring, filterkeys=filterkeys)
        for funcname in tsastats[key][value_key].keys():
            if funcname in ret_data:
                ret_data[funcname].append((datestring, tsastats[key][value_key][funcname]))
            else:
                ret_data[funcname] = [(datestring, tsastats[key][value_key][funcname]), ]
    for row in [{ "name" : funcname, "data" : ret_data[funcname]} for funcname in ret_data.keys()]:
        for key, value in row.items():
            print key, value


def main():
    project = "snmp"
    tablename = "hrStorageTable"
    key = ("srvcacdbp1.tilak.cc","E:\\ Label:New Volume  Serial Number f2423c17","HOST-RESOURCES-TYPES::hrStorageFixedDisk")
    value_key = "hrStorageUsed"
    month_report_hc(project, tablename, key, value_key)

if __name__ == "__main__":
    #main()
    cProfile.run("main()")
