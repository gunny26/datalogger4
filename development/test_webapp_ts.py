#!/usr/bin/python3
import datetime
import datalogger3
from datalogger3.b64 import b64eval

def get_ts(*args):
    project, tablename, datestring, value_keyname, index_key_b64 = args[:5]
    index_key = b64eval(index_key_b64)
    assert project in dl.get_projects()
    assert tablename in dl.get_tablenames(project)
    dl.setup(project, tablename, datestring)
    ts = dl["tsa", index_key]
    print(ts)
    # get utc offset for this timestamp
    # TODO: use ts_keyname
    first_ts = ts[0, str(dl.ts_keyname)]
    for entry in ts:
        print(entry)
    utc_offset = (datetime.datetime.fromtimestamp(first_ts) - datetime.datetime.utcfromtimestamp(first_ts)).total_seconds()
    hc_data = [] # must be list to work in highcharts
    hc_data.append({
        "name" : index_key,
        "data" : [((ts[index, str("ts")] + utc_offset) * 1000, ts[index, str(value_keyname)]) for index, entry in enumerate(ts)],
    })
    return hc_data

def get_tsastats(*args):
    project, tablename, datestring, value_keyname = args[:4]
    assert project in dl.get_projects()
    assert tablename in dl.get_tablenames(project)
    dl.setup(project, tablename, datestring)
    tsastats = dl["tsastats"].stats
    for index_key, tsstats in dl["tsastats"].stats.items():
        print(index_key, tsstats)
#
# https://datalogger-api.tirol-kliniken.cc/rest/v3/js_ts/haproxy/http_host/2018-05-14/rsp_2xx/KHUnaW50cmFuZXQudGlyb2wta2xpbmlrZW4uY2MnLCk=
dl = datalogger3.DataLogger("/var/rrd")
# print(get_ts("haproxy", "http_host", "2018-05-14", "rsp_2xx", "KHUnaW50cmFuZXQudGlyb2wta2xpbmlrZW4uY2MnLCk="))
print(get_tsastats("haproxy", "http_host", "2018-05-14", "rsp_2xx"))
