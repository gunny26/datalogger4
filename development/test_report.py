#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import json
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLoggerWeb as DataLoggerWeb
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from commons import *

def tsastats_remove_by_value(tsastats, value_key, stat_func_name, value):
    for key, tsstats in tsastats.items():
        if tsstats[value_key][stat_func_name]  == value:
            del tsastats[key]

def csv_to_table(csvdata, t_id=None, t_class=None):
    outbuffer = []
    t_id_str = ""
    if t_id is not None:
        t_id_str = "id=" + t_id
    t_class_str = ""
    if t_class is not None:
        t_class_str = "class=" + t_class
    outbuffer.append("<table %s %s><thead><tr>" % (t_id_str, t_class_str))
    [outbuffer.append("<th>%s</th>" % header) for header in csvdata[0]]
    outbuffer.append("</tr></thead><tbody>")
    for values in csvdata[1:]:
        outbuffer.append("<tr>")
        [outbuffer.append("<td>%s</td>" % value) for value in values]
        outbuffer.append("</tr>")
    outbuffer.append("</tbody></table>")
    return outbuffer

def csv_to_wiki(csvdata):
    outbuffer = []
    outbuffer.append("| " + " | ".join([("*%s*" % header) for header in csvdata[0]]) + " |")
    for values in csvdata[1:]:
        outbuffer.append("| " + " | ".join(["%s" % value for value in values]) + " |")
    return outbuffer

def tsastats_to_csv(tsastats, stat_func_name, sortkey=None, reverse=True):
    outbuffer = []
    outbuffer.append(tsastats.index_keys + tsastats.value_keys)
    data = None
    if sortkey is not None:
        data = sorted(tsastats.items(), key=lambda item: item[1][sortkey][stat_func_name], reverse=True)
    else:
        data = tsastats.items()
    for key, value in data:
        values = list(key) + [value[value_key][stat_func_name] for value_key in tsastats.value_keys]
        outbuffer.append(values)
    return outbuffer

def groupby(tsastat, index_keys):
    """
    group tsastat by index_keys, which are a subset of the original index_keys

    the grouping functions are predefined, it makes no sense to make this variable

    parameters:
    tsastat <TimeseriesArrayStats>
    index_keys <tuple>

    returns:
    <TimeseriesArrayStats>
    """
    group_funcs = {
        "sum" : lambda a, b: a + b,
        "avg" : lambda a, b: (a + b) / 2,
        "min" : min,
        "max" : max,
        "count" : lambda a, b: a + b,
        "std" : lambda a, b: (a + b) / 2,
        "median" : lambda a, b: (a + b) / 2,
        "mean" : lambda a, b: (a + b) / 2,
        "last" : lambda a, b: (a + b) / 2,
        "first" : lambda a, b: (a + b) / 2,
    }
    try:
        assert all(index_key in tsastat.index_keys for index_key in index_keys)
    except AssertionError:
        logging.error("All given index_keys have to be in tsastat.index_keys")
        return
    # intermediate data
    data = {}
    for key in tsastat.keys():
        key_dict = dict(zip(tsastat.index_keys, key))
        group_key = tuple((key_dict[key] for key in index_keys))
        if group_key not in data:
            data[group_key] = tsastat[key].stats
        else:
            # there is something to group
            for value_key in tsastat[key].keys():
                for stat_func, value in tsastat[key][value_key].items():
                    # group values by function
                    grouped_value = group_funcs[stat_func](value, data[group_key][value_key][stat_func])
                    # store
                    data[group_key][value_key][stat_func] = grouped_value
    # get to same format as TimeseriesArrayStats.to_json returns
    outdata = [tsastat.index_keys, tsastat.value_keys, ]
    outdata.append([(key, json.dumps(value)) for key, value in data.items()])
    # use TimeseriesArrayStats.from_json to get to TimeseriesArrayStats
    # object
    new_tsastat = TimeseriesArrayStats.from_json(json.dumps(outdata))
    return new_tsastat

if __name__ == "__main__":
    datalogger = DataLoggerWeb(DATALOGGER_URL)
    #caches = datalogger.get_caches("sanportperf", "fcIfC3AccountingTable", datalogger.get_last_business_day_datestring())
    tsastats = datalogger.get_tsastats("sanportperf", "fcIfC3AccountingTable", datalogger.get_last_business_day_datestring())
    g_tsastat1 = groupby(tsastats, (u'hostname',))
    tsastats = datalogger.get_tsastats("sanportperf", "fcIfC3AccountingTable", datalogger.get_last_business_day_datestring())
    g_tsastat2 = tsastats.group_by_index_keys((u'hostname',))
    print(g_tsastat1.keys())
    print(g_tsastat2.keys())
    assert g_tsastat1 == g_tsastat2
    g_tsastat = groupby(tsastats, (u'ifDescr',))
    print(g_tsastat.keys())
    g_tsastat = groupby(tsastats, (u'hostname', u'ifDescr',))
    assert g_tsastat == tsastats
    print(g_tsastat.keys())
    #tsastats.remove_by_value(u'fcIfC3InOctets', "sum", 0.0)
    #csvdata = tsastats.to_csv("sum", u'fcIfC3OutOctets', reverse=True)
    #print("\n".join(csv_to_table(csvdata[:20])))
    #print("\n".join(csv_to_wiki(csvdata[:20])))
    #cProfile.run("main()")
