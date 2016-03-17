#!/usr/bin/python
"""
get sensor date from GLT export File
sort of power consumption, temperature, and climate data

there is a big utf-8 issue, because the keys are utf-8 encoded

decode early
unicode everywhere
encode late
"""
import requests
import datetime
import os

def get_data(url, headers):
    res = requests.request(u"GET", url)
    data = []
    result = res.text
    for line in res.text.split(u"\n")[1:]:
        if len(line) == 0:
            continue
        values = line.strip().split(u";")
        # convert custom datetest <str> to datetime object
        dt = datetime.datetime.strptime(values[0], '%d.%m.%Y %H:%M')
        # calculate seconds since 1970-01-01 Unixtimestamp
        values[0] = u"%d" % (dt - datetime.datetime(1970, 1, 1)).total_seconds()
        row_dict = dict(zip(headers, values))
        data.append(row_dict)
    return data

def main():
    basedir = u"/var/rrd"
    project = u"glt"
    tablename = u"energiemonitor"
    url = u"http://srvebien01.tilak.cc/energiemonitor_export.csv"
    raw_basedir = os.path.join(basedir, project, u"raw")
    if not os.path.isdir(raw_basedir):
        print u"creating directory for raw data"
        os.makedirs(raw_basedir)
    headers = [u'ts', u'standort', u'bezeichnung', u'wert', u'einheit']
    index_keys = [u'standort', u'bezeichnung']
    value_keys = [u'wert', ]
    ts_keyname = u'ts'
    delimiter = u"\t"
    outbuffer = {}
    for row_dict in get_data(url, headers):
        datestring = datetime.datetime.fromtimestamp(float(row_dict[u"ts"])).date().isoformat()
        outfilename = os.path.join(raw_basedir, u"%s_%s.csv" % (tablename, datestring))
        fh = None
        if not os.path.isfile(outfilename):
            # if this file will be created, write header in first line
            fh = open(os.path.join(raw_basedir, outfilename), "w")
            # enode late
            fh.write(delimiter.join(headers).encode("utf-8") + "\n")
        else:
            fh = open(os.path.join(raw_basedir, outfilename), "a")
        # encode late
        fh.write(delimiter.join([row_dict[key] for key in headers]).encode("utf-8") + "\n")
        fh.close()

if __name__ == "__main__":
    main()
