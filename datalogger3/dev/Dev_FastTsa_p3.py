#!/usr/bin/python3
import os
import gzip
import json
import time
import concurrent.futures
import threading
from b64 import b64encode

def get_ts_writer(ts_keyname, value_keynames):
    def ts_writer(filename, key, data):
        firstrow = True
        with gzip.open(filename, "wt") as outfile:
            for ts in sorted(data.keys()):
                if firstrow:
                    # print(key, b64encode(key), ts, tsa[key][ts])
                    outfile.write(";".join([ts_keyname,] + value_keynames) + "\n")
                    firstrow = False
                outfile.write(";".join([str(ts), ] + [str(value) for value in data[ts]]) + "\n")
    return ts_writer

def get_ts_buffer_writer(ts_keyname, value_keynames):
    def ts_buffer_writer(filename, key, data):
        outbuffer = []
        firstrow = True
        for ts in sorted(data.keys()):
            if firstrow:
                # print(key, b64encode(key), ts, tsa[key][ts])
                outbuffer.append(";".join([ts_keyname,] + value_keynames))
                firstrow = False
            outbuffer.append(";".join([str(ts), ] + [str(value) for value in data[ts]]))
        with gzip.open(filename, "wt") as outfile:
            outfile.write("\n".join(outbuffer))
    return ts_buffer_writer

def get_raw_reader(filename, delimiter, tsa_def):
    index_keynames = tsa_def["index_keys"]
    value_keynames = tsa_def["value_keys"]
    ts_keyname = tsa_def["ts_key"]
    def inner():
        tsa = {}
        with gzip.open(filename, "rt") as infile:
            firstrow = True
            fileheaders = None
            for row in infile: # row is already stripped
                if firstrow:
                    fileheaders = row.strip().split(delimiter)
                    firstrow = False
                else:
                    data_dict = dict(zip(fileheaders, row.strip().split(delimiter))) # row_dict
                    ts = float(data_dict[ts_keyname])
                    key = tuple([data_dict[index_key] for index_key in index_keynames])
                    values = [float(data_dict[value_key]) for value_key in value_keynames]
                    if key not in tsa:
                        tsa[key] = {}
                    tsa[key][ts] = values
                    # print(key, ts, values)
        return tsa
    return inner

def get_raw_buffer_reader(filename, delimiter, tsa_def):
    index_keynames = tsa_def["index_keys"]
    value_keynames = tsa_def["value_keys"]
    ts_keyname = tsa_def["ts_key"]
    def inner():
        tsa = {}
        with gzip.open(filename, "rt") as infile:
            firstrow = True
            fileheaders = None
            for row in infile.read().split("\n"):
                if not row:
                    continue
                elif firstrow:
                    fileheaders = row.strip().split(delimiter)
                    firstrow = False
                else:
                    data_dict = dict(zip(fileheaders, row.strip().split(delimiter))) # row_dict
                    ts = float(data_dict[ts_keyname])
                    key = tuple([data_dict[index_key] for index_key in index_keynames])
                    values = [float(data_dict[value_key]) for value_key in value_keynames]
                    if key not in tsa:
                        tsa[key] = {}
                    tsa[key][ts] = values
                    # print(key, ts, values)
        return tsa
    return inner

def fast_tsa(raw_filename, cachedir, tsa_def, project, tablename, datestring):
    # read input data
    starttime = time.time()
    print("starting read")
    raw_reader = get_raw_reader(raw_filename, "\t", tsa_def)
    tsa = raw_reader()
    print("done in %0.2f" % (time.time() - starttime))
    starttime = time.time()
    print("dumping individual TS files")
    # output TS data
    ts_writer = get_ts_buffer_writer(tsa_def["ts_key"], tsa_def["value_keys"])
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for key in tsa:
            firstrow = True
            ts_filename = "ts_" + b64encode(key) + ".csv.gz"
            filename = os.path.join(cachedir, ts_filename)
            tsa_def["ts_filenames"].append(ts_filename)
            # print("filename : ", filename)
            futures.append(executor.submit(ts_writer, filename, key, tsa[key]))
        for future in concurrent.futures.wait(futures): # wait for futures to complete
            pass
    # output tsa structure
    print("done in in %0.2f" % (time.time() - starttime))
    tsa_filename = "tsa_" + b64encode(tsa_def["index_keys"]) + ".json"
    print("dumping tsa to ", tsa_filename)
    json.dump(tsa_def, open(os.path.join(cachedir, tsa_filename), "wt"), indent=4)


def main():
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
    # new TSA structure
    tsa_def = {
        "index_keys": index_keynames,
        "ts_filenames": [],
        "ts_key": ts_keyname,
        "value_keys": value_keynames
    }
    raw_filename = os.path.join(basedir, "%s_%s.csv.gz" % (tablename, datestring))
    fast_tsa(raw_filename, cachedir, tsa_def, project, tablename, datestring)
    print("done in %0.2f" % (time.time() - starttime))

if __name__ == "__main__":
    main()
