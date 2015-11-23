#!/usr/bin/python
from __future__ import print_function
import cProfile
import copy
import sys
import gc
import logging
logging.basicConfig(level=logging.INFO)
from datalogger import DataLoggerWeb as DataLoggerWeb
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

if __name__ == "__main__":
    datalogger = DataLoggerWeb(DATALOGGER_URL)
    #caches = datalogger.get_caches("sanportperf", "fcIfC3AccountingTable", datalogger.get_last_business_day_datestring())
    tsastats = datalogger.get_tsastats("sanportperf", "fcIfC3AccountingTable", datalogger.get_last_business_day_datestring())
    tsastats.remove_by_value(u'fcIfC3InOctets', "sum", 0.0)
    csvdata = tsastats.to_csv("sum", u'fcIfC3OutOctets', reverse=True)
    print("\n".join(csv_to_table(csvdata[:20])))
    print("\n".join(csv_to_wiki(csvdata[:20])))
    #cProfile.run("main()")
