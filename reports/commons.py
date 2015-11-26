#!/usr/bin/python
#import datetime
#import time
import logging
#import os
#import cPickle
#import hashlib
#from datalogger import DataLoggerHelper as dh
#from datalogger import DataLogger as DataLogger
#from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
import tilak_wiki

BASEDIR = "/var/rrd"
DATALOGGER_URL = "http://srvmgdata1.tilak.cc/DataLogger"

def get_header(project, tablename):
    wikitext = ""
    wikitext += "---+ %s %s Statistics\n" % (project, tablename)
    wikitext += "%TOC%\n"
    return wikitext

def get_report_infos(project, tablename, tsastat):
    wikitext = ""
    wikitext += "---++ Informations\n"
    wikitext += "| Project | %s |\n" % project
    wikitext += "| Tablename | %s |\n" % tablename
    wikitext += "| Number of unique keys found | %s |\n" % len(tsastat)
    return wikitext

def get_raw_stats(project, tablename, tsastat, stat_func, keyfunc):
    wikitext = ""
    wikitext += "---++ %s Statistics ungroupd\n" % tablename
    wikitext += "Table of data ungrouped and aggrated by %s over time.\n" % stat_func
    wikitext += get_wiki_table(tsastat, stat_func, keyfunc)
    return wikitext

def get_ungrouped_stats(project, tablename, tsastat, keyfunc):
    wikitext = ""
    wikitext += "---+ Ungrouped-Data Statistics\n"
    wikitext += "Here are some statistical breakdowns for every index combination\n"
    for value_key in tsastat.value_keys:
        wikitext += "---++ %s Statistics of %s\n" % (tablename, value_key)
        stat_dict = tsastat.get_stats(value_key)
        wikitext += get_wiki_dict_table(stat_dict, keyfunc)
    return wikitext

def get_grouped_stats(project, tablename, datestring, tsastat, keyfunc):
    #datalogger = DataLogger(BASEDIR, project, tablename)
    wikitext = ""
    wikitext += "---+ Grouped-Data Statistics\n"
    wikitext += "These statistics are grouped by some index_key, in sql something like select sum(value_key) from ... group by index_key\n"
    for subkey in tsastat.index_keys:
        wikitext += "---++ Grouped by %s\n" % subkey
        for value_key in tsastat.value_keys:
            wikitext += "---+++ %s Statistics of %s grouped by %s\n" % (tablename, value_key, subkey)
            wikitext += "This table is grouped by %s field using %s\n" % (subkey, "lambda a: sum(a)")
            g_tsastat = tsastat.group_by_index_keys((subkey,))
            wikitext += get_wiki_dict_table(g_tsastat.get_stats(value_key), keyfunc)
    return wikitext

def get_total_stats(project, tablename, datestring, tsastat, keyfunc):
    #datalogger = DataLogger(BASEDIR, project, tablename)
    wikitext = ""
    wikitext += "---+ Total-Data Statistics\n"
    for value_key in tsastat.value_keys:
        wikitext += "---++ %s Total Statistics of %s\n" % (tablename, value_key)
        wikitext += "This table is a summation of all data for field using %s\n" % "lambda a: sum(a)"
        g_tsastat = tsastat.group_by_index_keys(())
        wikitext += get_wiki_dict_table(g_tsastat.get_stats(value_key), keyfunc)
    return wikitext

def get_wiki_table(tsastat, stat_func_name, keyfunc):
    """
    tsa <TimeseriesArray> Object which hold data and statistics for many index_keys
    time_func <str> Function to aggregate Timeseries Data over Time

    returns foswiki string which represents a table
    """
    #index_keys = tsastat.index_keys
    #value_keys = tsastat.value_keys
    wikitext = "| " + " | ".join(("*%s*" % index_key for index_key in tsastat.index_keys)) + " | " + " | ".join(("*%s*" % value_key for value_key in tsastat.value_keys)) + " |\n"
    for key in tsastat.keys():
        try:
            wikitext += "| %s |  %s |\n" % (" | ".join(keyfunc(key)), " |  ".join(("%0.2f" % tsastat[key][value_key][stat_func_name] for value_key in tsastat.value_keys)))
        except StandardError as exc:
            logging.exception(exc)
            logging.info(key)
    return wikitext

def get_wiki_dict_table(data, keyfunc):
    """
    data <dict> input data in dict form

    returns fowiki formatted string which represents a table
    """
    wikitext = ""
    firstrow = True
    for key in data:
        fields = sorted(data[key].keys()) # get exact this order of fields
        if firstrow is True:
            wikitext += "| *key* | "+ " | ".join(("*%s*" % field for field in fields)) + " |\n"
            firstrow = False
        wikitext += "| %s |  %s |\n" % (keyfunc(key), " |  ".join(("%0.1f" % data[key][field] for field in fields)))
    return wikitext

#def get_yesterday_datestring():
#    return(datetime.date.fromtimestamp(time.time() - 60 * 60 * 24).isoformat())

#def get_last_business_day_datestring():
#    """
#    returns last businessday datestring, ignoring Feiertage
#    """
#    last_business_day = datetime.date.today()
#    shift = datetime.timedelta(max(1, (last_business_day.weekday() + 6) % 7 - 3))
#    last_business_day = last_business_day - shift
#    return(last_business_day.isoformat())

def raw_keyfuncgen(project, tablename, datestring):
    """
    generic keyfunc generator for raw tables
    """
    def keyfunc(keys):
        #logging.info(keys)
        newkeys = list(keys)
        #link = "<a href=http://srvmghomer.tilak.cc/rrd/static/%s_%s.html" % (project, tablename)
        newkeys[0] = "<a href=http://srvmgdata1.tilak.cc/static/develop3.html"
        newkeys[0] += "?project=%s" % project
        newkeys[0] += "&tablename=%s" % tablename
        newkeys[0] += "&datestring=%s" % datestring
        newkeys[0] += "&keys=%s" % ",".join(keys)
        newkeys[0] += " target=_blank>"
        newkeys[0] += "%s" % keys[0]
        newkeys[0] += "</a>"
        return tuple(newkeys)
    return keyfunc

def ungrouped_keyfuncgen(project, tablename, datestring):
    """
    get a tuple of index_keys and returns one string
    """
    def keyfunc(keys):
        #logging.info(keys)
        newkeys = list(keys)
        newkeys[0] = "<a href=http://srvmgdata1.tilak.cc/static/develop3.html"
        newkeys[0] += "?project=%s" % project
        newkeys[0] += "&tablename=%s" % tablename
        newkeys[0] += "&datestring=%s" % datestring
        newkeys[0] += "&keys=%s" % ",".join(keys)
        newkeys[0] += " target=_blank>"
        newkeys[0] += "%s" % keys[0]
        newkeys[0] += "</a>"
        return " ".join(newkeys)
    return keyfunc

def standard_wiki_report(project, tablename, datestring, tsastat, tsastat_grouped, wikiname, raw_stat_func="avg"):
    ws = tilak_wiki.TilakWikiSender()
    wikitext = ""
    wikitext += get_header(project, tablename)
    wikitext += ws.get_proclaimer()
    wikitext += get_report_infos(project, tablename, tsastat)
    # RAW Data
    wikitext += get_raw_stats(project, tablename, tsastat, stat_func=raw_stat_func, keyfunc=raw_keyfuncgen(project, tablename, datestring))
    # slice data to relevant columns
    # Un-Groupded Data
    wikitext += get_ungrouped_stats(project, tablename, tsastat_grouped, ungrouped_keyfuncgen(project, tablename, datestring))
    # grouped Data
    wikitext += get_grouped_stats(project, tablename, datestring, tsastat_grouped, keyfunc=lambda a: a)
    # Totals
    wikitext += get_total_stats(project, tablename, datestring, tsastat_grouped, keyfunc=lambda a: a)
    ws.send("Systembetrieb", wikiname, wikitext)

def simple_wiki_report(project, tablename, datestring, tsastat, wikiname, raw_stat_func="avg"):
    ws = tilak_wiki.TilakWikiSender()
    wikitext = ""
    wikitext += get_header(project, tablename)
    wikitext += ws.get_proclaimer()
    wikitext += get_report_infos(project, tablename, tsastat)
    # RAW Data
    wikitext += get_raw_stats(project, tablename, tsastat, stat_func=raw_stat_func, keyfunc=raw_keyfuncgen(project, tablename, datestring))
    ws.send("Systembetrieb", wikiname, wikitext)
