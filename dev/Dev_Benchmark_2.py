#!/usr/bin/python3
import gzip
import time
from b64 import b64encode

print("testing binary read")
starttime = time.time()
with gzip.open("testdata/virtualMachineCpuStats6_2018-04-19.csv.gz", "rb") as infile:
    for row in infile:
        data = row.strip()
print("done in %0.2f" % (time.time() - starttime))
print("testing text read")
tsa = {}
starttime = time.time()
headers = ["ts", "key1", "key2", "value1", "value2", "value3", "value4"]
ts_keyname = headers[0]
index_keynames = headers[1:3]
value_keynames = headers[3:]
with gzip.open("testdata/virtualMachineCpuStats6_2018-04-19.csv.gz", "rt") as infile:
    skipfirst = True
    for row in infile:
        if skipfirst:
            skipfirst = False
            continue
        data_dict = dict(zip(headers, row.strip().split("\t"))) # row_dict
        ts = float(data_dict[ts_keyname])
        key = tuple([data_dict[key] for key in index_keynames])
        values = [float(data_dict[key]) for key in value_keynames]
        if key not in tsa:
            tsa[key] = {}
        tsa[key][ts] = values
        # print(key, ts, values)
print("done in %0.2f" % (time.time() - starttime))
for key in tsa:
    firstrow = True
    for ts in sorted(tsa[key].keys())[:10]:
        if firstrow:
            filename = "ts_" + b64encode(key) + ".csv.gz"
            print("filename : ", filename) 
            print(key, b64encode(key), ts, tsa[key][ts])
            print(";".join([ts_keyname,] + value_keynames))
            firstrow = False
        print(";".join([str(ts), ] + [str(value) for value in tsa[key][ts]]))
    break
