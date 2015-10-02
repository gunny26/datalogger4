#!/usr/bin/python
import time
import json
import datetime
import logging
logging.basicConfig(level=logging.DEBUG)
import os

DAY = 60 * 60 * 24
TS = {
    'hourly' : 60 * 60,
    'daily' : DAY,
    'weekly' : 7 * DAY,
    'monthly' : 28 * DAY,
    'yearly' : 364 * DAY,
}

basedir = "/var/rrd"
ts_keyname = "ts"
delimiter = "\t"
index_keynames = ("sensor",)
value_keynames = ("counter", "first_ts", "pulses", "pulses_5min", "pulses_day", "pulses_week", "pulses_month") 
headers = index_keynames + (ts_keyname, ) + value_keynames
inheaders = (ts_keyname, ) + value_keynames

def read_data(filename, start_ts, stop_ts=time.time()):
    """
    read one line from raw data, start and stop in timestamp as int
    """
    inbuffer = open(filename, "rb").read().split("\n")
    for row in inbuffer:
        try:
            row_dict = dict(zip(inheaders, (float(value) for value in row.split("\t"))))
            row_dict["sensor"] = "sensor0"
            if start_ts < row_dict[ts_keyname] < stop_ts:
                yield row_dict
        except ValueError as exc:
            #logging.exception(exc)
            logging.error("ValueError on line %s", row)

def main():
    project = "energy"
    tablename = "em1010"
    raw_basedir = os.path.join(basedir, project, "raw")
    #1427016303 884 1426751403  9   41  1144    3123    3123
    filename = os.path.join(raw_basedir, "em1010_log.csv")
    outbuffer = {}
    for data in read_data(filename, time.time() - 365*DAY):
        outfilename = "%s_%s.csv" % (tablename, datetime.date.fromtimestamp(data[ts_keyname]).isoformat())
        if outfilename not in outbuffer:
            outbuffer[outfilename] = []
            outbuffer[outfilename].append(delimiter.join(headers))
        outbuffer[outfilename].append(delimiter.join((str(data[header]) for header in headers)))
    for outfilename, data in outbuffer.items():
        open(os.path.join(raw_basedir, outfilename), "wb").write("\n".join(data))

if __name__ == "__main__":
    main()
