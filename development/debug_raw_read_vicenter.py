#!/usr/bin/python

import gzip
import json
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

meta_data = json.load(open("/var/rrd/vicenter/meta/hostSystemMemoryStats.json"))
print meta_data
#last_value = {}
with gzip.open("/var/rrd/vicenter/raw/hostSystemMemoryStats_2017-03-10.csv.gz") as raw:
    for lineno, line in enumerate(raw.read().split("\n")[1:]):
        if line.startswith("#") or len(line) == 0:
            continue
        fields = line.split("\t")
        # fields.remove("") # remove empty keys
        try:
            assert len(fields) == len(meta_data["headers"])
            row_dict = dict(zip(meta_data["headers"], fields))
            index_key = tuple((row_dict[key] for key in meta_data["index_keynames"]))
            logging.info(index_key)
            #if index_key != ('srvmakprd1.tilak.cc', '0'):
            #    continue
            logging.info(index_key, row_dict)
            #print index_key, row_dict["ifHCInOctets"], last_value[index_key], 2**64
            #if row_dict["ifHCInOctets"] < last_value[index_key]:
            #    print "overflow detected %f to %f" % (last_value[index_key], row_dict["ifHCInOctets"])
            #    raise StandardError()
            #last_value[index_key] = row_dict["ifHCInOctets"]
            #print lineno, row_dict["ts"]
        except AssertionError as exc:
            logging.error(fields)
            logging.error("found %d fields, should be %d", len(fields), len(meta_data["headers"]))
