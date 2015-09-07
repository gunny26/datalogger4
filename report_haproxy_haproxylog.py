#!/usr/bin/python
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import tilak_wiki
from tilak_datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import DataLoggerHelper as dh
from commons import *

def group_tsastat(tsastat, group_keys, value_keys, group_func):
    """
    groups existing tsastat objects by group_keys with group_func
    only keys in value_keys are in returned dict

    parameters:
    tsastat <TimeseriesArrayStats>
    group_keys <tuple>
    value_keys <tuple>
    group_func <func> is applied to aggregate data

    returns:
    <dict>
    """
    ret_dict = {}
    index_keys = tsastat.index_keys
    for key in tsastat.stats.keys():
        key_dict = dict(zip(index_keys, key))
        for value_key in value_keys:
            key_dict[value_key] = tsastat[key].stats[value_key]["sum"]
        group_values = [key_dict[subkey] for subkey in group_keys]
        subkey = tuple([keyvalue for keyvalue in key if keyvalue in group_values])
        if subkey not in ret_dict:
            ret_dict[subkey] = dict(zip(value_keys, (key_dict[value_key] for value_key in value_keys)))
        else:
            ret_dict[subkey] = dict(zip(value_keys, (group_func(key_dict[value_key], ret_dict[subkey][value_key]) for value_key in value_keys)))
    return ret_dict

def get_group_tsastat_wikitable(tsastat, group_keys, value_keys, group_func):
    wikitext = []
    ret_dict = group_tsastat(tsastat, group_keys, value_keys, group_func)
    wikitext.append("---++ Statistics grouped by %s" % str(group_keys))
    wikitext.append("| *" + "* | *".join(group_keys) + "* | *" + "* | *".join(value_keys) + "* |")
    for key, values in ret_dict.items():
        wikitext.append("| " + " | ".join(key) + " | " + " | ".join([str(values[value_key]) for value_key in value_keys]) + " |")
        #print key, values
    return wikitext

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa.remove_col(u"srv_queue")
    tsa.remove_col(u"backend_queue")
    tsa.remove_col(u"actconn")
    tsa.remove_col(u"feconn")
    tsa.remove_col(u"beconn")
    tsa.remove_col(u"srv_conn")
    tsa.remove_col(u"retries")
    tsa.remove_col(u"tq")
    tsa.remove_col(u"tw")
    tsa.remove_col(u"tc")
    tsa.remove_col(u"tr")
    tsa.remove_col(u"tt")
    # get TimeseriesArrayStats Object
    tsastat = TimeseriesArrayStats(tsa)
    ws = tilak_wiki.TilakWikiSender()
    wikitext = []
    wikitext.append(get_header(datalogger))
    wikitext.append(ws.get_proclaimer())
    wikitext.extend(get_group_tsastat_wikitable(tsastat, ("http_host", ), tsastat.value_keys, lambda a, b : a + b))
    wikitext.extend(get_group_tsastat_wikitable(tsastat, ("client_ip", ), tsastat.value_keys, lambda a, b : a + b))
    wikitext.extend(get_group_tsastat_wikitable(tsastat, ("server_name", ), tsastat.value_keys, lambda a, b : a + b))
    wikiname = "%s%s" % (datalogger.get_wikiname(), datestring)
    ws.send("Systembetrieb", wikiname, "\n".join(wikitext))
    #simple_wiki_report(datalogger, datestring, tsa, raw_stat_func="sum")

def main():
    project = "haproxy"
    tablename = "haproxylog"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    datestring = "2015-08-31"
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
