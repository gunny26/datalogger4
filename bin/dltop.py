#!/usr/bin/python3
"""
This is a more efficient version, since it does not read the entire
file
"""
import sys
import os
import time
import datetime
import threading
import datalogger3

class LiveReaderData(object):

    def __init__(self, ts, initial, mode="last"):
        self.previous = (ts-0.1, dict(initial)) # to prevent division by zero
        self.last = (ts, dict(initial))
        self.epochs = 1
        self.__mode = mode
        self.__ts = ts

    def set_mode(self, mode):
        self.__mode = mode

    def update(self, ts, value_dict):
        self.previous = self.last
        self.last = (ts, value_dict)

    def __getitem__(self, key):
        if self.__mode == "diff":
            timediff = self.last[0] - self.previous[0]
            if timediff == 0:
                return 0
            else:
                return (self.last[1][key] - self.previous[1][key]) / timediff
        return self.last[key]

    def keys(self):
        return self.last.keys()

    def values(self):
        return self.last.values()

    def items(self):
        return self.last.items()


def live_reader(fname, data):
    data["filename"] = fname
    firstline = True
    with open(fname, "rt") as infile:
        print("reading live")
        data["mtime"] = 0.0
        while True:
            if data["mtime"] == os.stat(fname).st_mtime:
                time.sleep(2)
                continue
            for line in infile.read().splitlines():
                if firstline is True:
                    firstline = False
                    continue
                try:
                    cols = line.split(data["delimiter"])
                    row_dict = dict(zip(data["headers"], cols))
                    index_dict = dict([(key, row_dict[key]) for key in data["index_keynames"]])
                    value_dict = dict([(key, float(row_dict[key])) for key in data["value_keynames"]])
                    full_dict = dict(index_dict)
                    full_dict.update(value_dict)
                    ts = float(row_dict[data["ts_keyname"]])
                    index_key = tuple([index_dict[key] for key in data["index_keynames"]])
                    data["max_ts"] = ts
                    if any((pattern in index_key for pattern in data["index_key_filter"])):
                        continue
                    with display_lock:
                        # calculate diffs
                        if index_key in data["data"]:
                            data["data"][index_key].update(ts, value_dict)
                        else:
                            data["data"][index_key] = LiveReaderData(ts, value_dict, mode="diff")
                except ValueError:
                    pass
                except IndexError:
                    pass
            data["mtime"] = os.stat(fname).st_mtime

def live_printer(data):
    my_last_ts = 0.0
    index_format_str = "{:<20} | " * len(data["index_keynames"])
    value_header_str = "{:>17} | " * len(data["value_keynames"])
    header_format_str = index_format_str + " | " + value_header_str
    value_data_str = "{:>17.2f} | " * len(data["value_keynames"])
    while True:
        # clear screen and set cursor left/top
        if my_last_ts != data["max_ts"] or my_last_ts is 0.0:
            with display_lock:
                os.system('cls' if os.name == 'nt' else 'clear')
                firstrow = True
                print("project                  : %(project)s" % data)
                print("tablename                : %(tablename)s" % data)
                print("datestring               : %(datestring)s" % data)
                print("watching file            : %(filename)s" % data)
                print("file mtime               : %s" % datetime.datetime.fromtimestamp(data["mtime"]))
                print("last refresh             : %s" % datetime.datetime.today())
                print("last timestamp from file : %s" % datetime.datetime.fromtimestamp(data["max_ts"]))
                print("num of entries           : %d" % len(data["data"]))
                for key, value in sorted(data["data"].items(), key = lambda a : a[1][data["sort_key"]], reverse=True)[:40]:
                    if firstrow:
                        header_str = header_format_str.format(*(data["index_keynames"] + data["value_keynames"]))
                        print("-" * len(header_str))
                        print(header_str)
                        print("-" * len(header_str))
                        firstrow = False
                    index_str = index_format_str.format(*key)
                    value_str = value_data_str.format(*[value[key] for key in data["value_keynames"]])
                    print(index_str + " | " + value_str)
                my_last_ts = data["max_ts"]
        time.sleep(10)


display_lock = threading.Lock() # set to not modify dict until output if finished

if __name__ == "__main__":
    basedir = "/var/rrd"
    datestring = datetime.date.today().isoformat()
    dl = datalogger3.DataLogger(basedir)
    dl.setup("ucs", "ifXTable", dl.get_last_business_day_datestring())
    data = {
        "project" : dl.project,
        "datestring" : datestring,
        "tablename" : dl.tablename,
        "delimiter" : dl.delimiter,
        "headers" : dl.headers,
        "index_keynames" : dl.index_keynames,
        "value_keynames" : dl.value_keynames,
        "ts_keyname" : dl.ts_keyname,
        "max_ts" : 0,
        "data" : {},
        "mtime" : 0.0,
        "filename" : None,
        "sort_key" : dl.value_keynames[0],
        "index_key_filter" : ["System Idle Process", ]
    }
    fname = os.path.join(basedir, dl.project, "raw", "%s_%s.csv" % (dl.tablename, datestring))
    reader = threading.Thread(target=live_reader, args=(fname, data))
    reader.start()
    printer = threading.Thread(target=live_printer, args=(data, ))
    printer.start()

