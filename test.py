#!/usr/bin/python

import os
import itertools
import copy

headers = None
field_types = {}
last_len = None
timestamp = None
data = {}
for rownum, row in enumerate(open("/var/rrd/ipstor/raw/vrsRealDevTable_2015-07-28.csv", "rb")):
    if headers is None:
        headers = row.strip().split("\t")
        p_headers =  copy.copy(headers)
        p_headers.remove("ts")
        p_headers.remove('vrsrdKBRead')
        p_headers.remove('vrsrdReadError')
        p_headers.remove('vrsrdSCSIReadCmd')
        p_headers.remove('vrsrdKBRead64')
        p_headers.remove('vrsrdKBWritten64')
        p_headers.remove('vrsrdOtherSCSICmd')
        p_headers.remove('vrsrdSCSIWriteCmd')
        p_headers.remove('vrsrdFirstSector')
        p_headers.remove('vrsrdFirstSector64')
        p_headers.remove('vrsrdKBWritten')
        p_headers.remove('vrsrdLastSector')
        p_headers.remove('vrsrdLastSector64')
        p_headers.remove('vrsrdWriteError')
        p_keys = []
        for length in range(2, len(p_headers)):
            keys = itertools.combinations(p_headers, length)
            for p_key in keys:
                p_keys.append(p_key)
        n_keys = []
        for p_key in p_keys:
            if "hostname" in p_key:
                n_keys.append(p_key)
        p_keys = n_keys
        for p_key in p_keys:
            print p_key
        print "Found %d possible key combinations" % len(p_keys)
        continue
    if row.startswith(headers[0]):
        print "new header line found"
    fields = row.split("\t")
    if last_len is None:
        last_len = len(fields)
    assert last_len == len(fields)
    for index, field in enumerate(fields):
        colname = headers[index]
        try:
            float(field)
            if field in field_types:
                assert field_types[colname] == "numeric"
            else:
                field_types[colname] = "numeric"
        except ValueError:
            str(fields)
            if field in field_types:
                assert field_types[colname] == "text"
            else:
                field_types[colname] = "text"
    col_dict = dict(zip(headers, fields))
    for p_key in p_keys:
#        print "trying %s" % str(p_key)
        p_values = tuple((col_dict[subkey] for subkey in p_key))
        if p_values in data:
            data[p_values]["hits"] += 1
        else:
            data[p_values]={
                "key" : p_key,
                "hits" : 1
            }
    if timestamp is None:
        timestamp = col_dict["ts"]
    else:
        if timestamp != col_dict["ts"]:
            break
    print rownum

print headers
print field_types
for p_key, struct in data.items():
    if struct["hits"] == 1:
        print struct["key"]
        print p_key, struct["hits"]


