#!/usr/bin/python3
import os
import gzip
import json
import time
import threading
from b64 import b64encode

def get_ts_writer(ts_keyname, value_keynames):
    def ts_writer(filename, key, data):
        firstrow = True
        with gzip.open(filename, "wt") as outfile:
            for ts in sorted(data.keys()):
                if firstrow:
                    # print(key, b64encode(key), ts, tsa[key][ts])
                    outfile.write(";".join([ts_keyname,] + value_keynames))
                    firstrow = False
                outfile.write(";".join([str(ts), ] + [str(value) for value in data[ts]]))
    return ts_writer

print("testing text read")
tsa = {}
starttime = time.time()
# definitions datalogger
basedir = "testdata"
cachedir = os.path.join(basedir, "cache")
project = "testproject"
tablename = "testtablename"
datestring = "2018-04-19"
delimiter = "\t"
# TSA Definition
headers = ["ts", "hostname", "instance", "cpu.idle.summation", "cpu.ready.summation", "cpu.used.summation", "cpu.wait.summation"]
ts_keyname = headers[0]
index_keynames = headers[1:3]
value_keynames = headers[3:]
tsa_def = {
    "index_keys": index_keynames,
    "ts_filenames": [],
    "ts_key": ts_keyname,
    "value_keys": value_keynames
}
# print(tsa_def)
# raw filename
raw_filename = os.path.join(basedir, "%s_%s.csv.gz" % (tablename, datestring))
# read input data
with gzip.open(raw_filename, "rt") as infile:
    firstrow = True
    fileheaders = None
    for row in infile:
        if firstrow:
            fileheaders = row.strip().split(delimiter)
            firstrow = False
            continue
        data_dict = dict(zip(fileheaders, row.strip().split(delimiter))) # row_dict
        ts = float(data_dict[ts_keyname])
        key = tuple([data_dict[index_key] for index_key in index_keynames])
        values = [float(data_dict[value_key]) for value_key in value_keynames]
        if key not in tsa:
            tsa[key] = {}
        tsa[key][ts] = values
        # print(key, ts, values)
print("read from raw done in %0.2f" % (time.time() - starttime))
starttime = time.time()
# output TS data
ts_writer = get_ts_writer(ts_keyname, value_keynames)
for key in tsa:
    firstrow = True
    ts_filename = "ts_" + b64encode(key) + ".csv.gz"
    filename = os.path.join(cachedir, ts_filename)
    tsa_def["ts_filenames"].append(ts_filename)
    # print("filename : ", filename)
    t = threading.Thread(target=ts_writer, args=(filename, key, tsa[key]))
    #t.daemon = True
    t.start()
# output tsa structure
tsa_filename = "tsa_" + b64encode(index_keynames) + ".json"
print("dumping tsa to ", tsa_filename)
json.dump(tsa_def, open(os.path.join(basedir, tsa_filename), "wt"), indent=4)
print("dumping tsa and indicidual ts done in %0.2f" % (time.time() - starttime))
