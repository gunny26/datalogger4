#!/usr/bin/python

import gzip
import json

meta_data = json.load(open("/var/rrd/ucs/meta/ifXTable.json"))
print meta_data
last_value = {}
with gzip.open("/var/rrd/ucs/raw/ifXTable_2016-02-05.csv.gz") as raw:
    for lineno, line in enumerate(raw.read().split("\n")[1:]):
        if line.startswith("#") or len(line) == 0:
            continue
        fields = line.split("\t")
        try:
            assert len(fields) == len(meta_data["headers"])
            row_dict = dict(zip(meta_data["headers"], fields))
            index_key = tuple((row_dict[key] for key in meta_data["index_keynames"]))
            if index_key != ('ucsfia-sr2-1-mgmt0', 'Vethernet885'):
                continue
            for key in meta_data["value_keynames"]:
                try:
                    row_dict[key] = float(row_dict[key])
                    if index_key not in last_value:
                        last_value[index_key] = 0L
                except ValueError as exc:
                    #print row_dict
                    pass
            print index_key, row_dict["ifHCInOctets"], last_value[index_key], 2**64
            if row_dict["ifHCInOctets"] < last_value[index_key]:
                print "overflow detected %f to %f" % (last_value[index_key], row_dict["ifHCInOctets"])
                raise StandardError()
            last_value[index_key] = row_dict["ifHCInOctets"]
            print lineno, row_dict["ts"]
        except AssertionError as exc:
            print "found %d fields, should be %d" % (len(fields), len(headers))
