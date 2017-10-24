#!/usr/bin/python

import gzip
import json

meta_data = json.load(open("/var/rrd/vicenter/meta/virtualMachineCpuStats6.json"))
print meta_data
last_value = {}
with gzip.open("/var/rrd/vicenter/raw/virtualMachineCpuStats6_2017-05-22.csv.gz") as raw:
    for lineno, line in enumerate(raw.read().split("\n")[1:]):
        if line.startswith("#") or len(line) == 0:
            continue
        fields = line.split("\t")
        try:
            assert len(fields) == len(meta_data["headers"])
            row_dict = dict(zip(meta_data["headers"], fields))
            index_key = tuple((row_dict[key] for key in meta_data["index_keynames"]))
            if index_key != ('srvapwsu02.tilak.cc', '0'):
                continue
            for key in meta_data["value_keynames"]:
                try:
                    row_dict[key] = float(row_dict[key])
                except ValueError as exc:
                    print row_dict
                    pass
            #if any((row_dict[value_keyname] > 20000.0 for value_keyname in meta_data["value_keynames"])):
            #    print row_dict
            if row_dict["cpu.used.summation"] > 20000.0:
                row_dict["cpu.used.percent"] = 100 * row_dict["cpu.used.summation"] / 20000
                print "%(ts)s\t%(hostname)s\t%(instance)s\t%(cpu.used.summation)s\t%(cpu.used.percent)s" % row_dict
                # print row_dict["ts"], row_dict["cpu.used.summation"], row_dict["cpu.used.percent"]
            #print index_key, row_dict["ifHCInOctets"], last_value[index_key], 2**64
            #if row_dict["ifHCInOctets"] < last_value[index_key]:
            #    print "overflow detected %f to %f" % (last_value[index_key], row_dict["ifHCInOctets"])
            #    raise StandardError()
            #last_value[index_key] = row_dict["ifHCInOctets"]
        except AssertionError as exc:
            print "found %d fields, should be %d" % (len(fields), len(headers))
