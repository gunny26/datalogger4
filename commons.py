#!/usr/bin/python
import datetime
import time
import logging
import os
import cPickle
import hashlib
from tilak_datalogger import DataLoggerHelper as dh
from tilak_datalogger import TimeseriesArrayStats as TimeseriesArrayStats
import tilak_wiki

BASEDIR = "/var/rrd"

def get_header(datalogger):
    wikitext = ""
    wikitext += "---+ %s %s Statistics\n" % (datalogger.get_project(), datalogger.get_tablename())
    wikitext += "%TOC%\n"
    return(wikitext)

def get_report_infos(datalogger, tsa):
    wikitext = ""
    wikitext += "---++ Informations\n"
    wikitext += "| earliest timestamp found | %s |\n"  % datetime.datetime.fromtimestamp(tsa.get_first_ts())
    wikitext += "| latest timestamp found | %s |\n"  % datetime.datetime.fromtimestamp(tsa.get_last_ts())
    wikitext += "| Project | %s |\n" % datalogger.get_project()
    wikitext += "| Tablename | %s |\n" % datalogger.get_tablename()
    wikitext += "| Number of unique keys found | %s |\n" % len(tsa)
    return(wikitext)

def get_raw_stats(datalogger, tsa, stat_func, keyfunc):
    wikitext = ""
    wikitext += "---++ %s Statistics ungroupd\n" % datalogger.get_tablename()
    wikitext += "Table of data ungrouped and aggrated by %s over time.\n" % stat_func
    wikitext += get_wiki_table(tsa, stat_func, keyfunc)
    return wikitext

def get_ungrouped_stats(datalogger, tsa, keyfunc):
    wikitext = ""
    wikitext += "---+ Ungrouped-Data Statistics\n"
    wikitext += "Here are some statistical breakdowns for every index combination\n"
    for value_key in tsa.get_value_keys():
        wikitext += "---++ %s Statistics of %s\n" % (datalogger.get_tablename(), value_key)
        tsa_stats = TimeseriesArrayStats(tsa)
        stat_dict = tsa_stats.get_stats(value_key)
        wikitext += get_wiki_dict_table(stat_dict, keyfunc)
    return(wikitext)

def get_grouped_stats(datalogger, tsa, keyfunc):
    wikitext = ""
    wikitext += "---+ Grouped-Data Statistics\n"
    wikitext += "These statistics are grouped by some index_key, in sql something like select sum(value_key) from ... group by index_key\n"
    for subkey in tsa.get_index_keys():
        wikitext += "---++ Grouped by %s\n" % subkey
        for value_key in tsa.get_value_keys():
            wikitext += "---+++ %s Statistics of %s grouped by %s\n" % (datalogger.get_tablename(), value_key, subkey)
            wikitext += "This table is grouped by %s field using %s\n" % (subkey, "lambda a: sum(a)")
            grouped_tsa = tsa.get_group_by_tsa((subkey,), group_func=lambda a: sum(a))
            tsa_stats = TimeseriesArrayStats(grouped_tsa)
            wikitext += get_wiki_dict_table(tsa_stats.get_stats(value_key), keyfunc)
    return(wikitext)

def get_total_stats(datalogger, tsa, keyfunc):
    wikitext = ""
    wikitext += "---+ Total-Data Statistics\n"
    for value_key in tsa.get_value_keys():
        wikitext += "---++ %s Total Statistics of %s\n" % (datalogger.get_tablename(), value_key)
        wikitext += "This table is a summation of all data for field using %s\n" % "lambda a: sum(a)"
        grouped_tsa = tsa.get_group_by_tsa((), group_func=lambda a: sum(a))
        tsa_stats = TimeseriesArrayStats(grouped_tsa)
        wikitext += get_wiki_dict_table(tsa_stats.get_stats(value_key), keyfunc)
    return(wikitext)

def get_wiki_table(tsa, time_func, keyfunc):
    """
    tsa <TimeseriesArray> Object which hold data and statistics for many index_keys
    time_func <str> Function to aggregate Timeseries Data over Time

    returns foswiki string which represents a table
    """
    wikitext = ""
    firstrow = True
    index_keys = tsa.get_index_keys()
    tsa_stat = TimeseriesArrayStats(tsa)
    for key in tsa.keys():
        if len(tsa[key]) == 0: # skip empty timeseries objects
            continue
        fields = sorted(tsa[key].get_headers()) # get exact this order of fields
        if firstrow is True:
            wikitext += "| " + " | ".join(("*%s*" % value for value in index_keys)) + " | " + " | ".join(("*%s*" % field for field in fields)) + " |\n"
            firstrow = False
        try:
            stat_dict = tsa_stat[key].get_stat(time_func)
            wikitext += "| %s |  %s |\n" % (" | ".join(keyfunc(key)), " |  ".join(("%0.2f" % stat_dict[field] for field in fields)))
        except StandardError as exc:
            logging.info(key)
            logging.exception(exc)
    return(wikitext)

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
    return(wikitext)

def get_yesterday_datestring():
    return(datetime.date.fromtimestamp(time.time() - 60 * 60 * 24).isoformat())

def get_last_business_day_datestring():
    """
    returns last businessday datestring, ignoring Feiertage
    """
    last_business_day = datetime.date.today()
    shift = datetime.timedelta(max(1,(last_business_day.weekday() + 6) % 7 - 3))
    last_business_day = last_business_day - shift
    return(last_business_day.isoformat())

def raw_keyfuncgen(datalogger, datestring):
    """
    generic keyfunc generator for raw tables
    """
    project = datalogger.get_project()
    tablename = datalogger.get_tablename()
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
        return(tuple(newkeys))
    return(keyfunc)

def ungrouped_keyfuncgen(datalogger, datestring):
    """
    get a tuple of index_keys and returns one string
    """
    project = datalogger.get_project()
    tablename = datalogger.get_tablename()
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
        return(" ".join(newkeys))
    return(keyfunc)

def standard_wiki_report(datalogger, datestring, tsa, tsa_grouped, raw_stat_func="avg", wikiname=None):
    ws = tilak_wiki.TilakWikiSender()
    wikitext = ""
    wikitext += get_header(datalogger)
    wikitext += ws.get_proclaimer()
    wikitext += get_report_infos(datalogger, tsa)
    # RAW Data
    wikitext += get_raw_stats(datalogger, tsa, stat_func=raw_stat_func, keyfunc=raw_keyfuncgen(datalogger, datestring))
    # slice data to relevant columns
    # Un-Groupded Data
    wikitext += get_ungrouped_stats(datalogger, tsa_grouped, ungrouped_keyfuncgen(datalogger, datestring))
    # grouped Data
    wikitext += get_grouped_stats(datalogger, tsa_grouped, keyfunc=lambda a: a)
    # Totals
    wikitext += get_total_stats(datalogger, tsa_grouped, keyfunc=lambda a: a)
    if wikiname is None:
        wikiname = datalogger.get_wikiname()
    ws.send("Systembetrieb", wikiname, wikitext)

def simple_wiki_report(datalogger, datestring, tsa, raw_stat_func="avg", wikiname=None):
    ws = tilak_wiki.TilakWikiSender()
    wikitext = ""
    wikitext += get_header(datalogger)
    wikitext += ws.get_proclaimer()
    wikitext += get_report_infos(datalogger, tsa)
    # RAW Data
    wikitext += get_raw_stats(datalogger, tsa, stat_func=raw_stat_func, keyfunc=raw_keyfuncgen(datalogger, datestring))
    if wikiname is None:
        wikiname = datalogger.get_wikiname()
    ws.send("Systembetrieb", wikiname, wikitext)

def dump_tsa(datalogger, datestring, tsa, global_cachedir):
    """
    use uniceod everywhere
    """
    project = u"%s" % datalogger.get_project()
    tablename = u"%s" % datalogger.get_tablename()
    for key in tsa.keys():
        md5 = hashlib.md5()
        md5.update(str((project, tablename, key, u"%s" % datestring)))
        subdir = os.path.join(global_cachedir, datestring)
        filename = "%s_%s_%s.dmp" % (project, tablename, md5.hexdigest())
        if not os.path.exists(subdir):
            os.mkdir(subdir)
        full_filename = os.path.join(subdir, filename)
        logging.info("dumping tsa key %s to filename %s", key, full_filename)
        cPickle.dump(tsa[key], open(full_filename, "wb"))

