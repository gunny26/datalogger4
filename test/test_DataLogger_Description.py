#!/usr/bin/python3
from __future__ import print_function
import unittest
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import gzip
import json
import yaml
import os
# own modules
import datalogger4 # to use assertIsInstance for testing
from datalogger4.DataLogger import DataLogger
from datalogger4.Timeseries import Timeseries
from datalogger4.TimeseriesArray import TimeseriesArray
from datalogger4.TimeseriesStats import TimeseriesStats
from datalogger4.TimeseriesArrayStats import TimeseriesArrayStats
from datalogger4.Quantile import QuantileArray
from datalogger4.Quantile import Quantile

class Test(unittest.TestCase):

    def setUp(self):
        self.basedir = "testdata"
        self.project = "mysql"
        self.tablename = "performance"
        self.datestring = "2018-04-01"
        self.datalogger = DataLogger(self.basedir)

    def notest__str__(self):
        print(self.datalogger)

    def test_read_meta_old(self):
        print("testing __getitem__")
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        print(json.dumps(dl.meta, indent=4))
        meta = dl.meta
        if "descriptions" in meta:
            description = meta["descriptions"]
            index_keynames = tuple([key for key in description if description[key]["coltype"] == "index"])
            print("index_keynames:", index_keynames)
            self.assertEqual(index_keynames, dl.index_keynames)
            value_keynames = tuple([key for key in description if description[key]["coltype"] == "value"])
            print("value_kenames:", value_keynames)
            self.assertEqual(sorted(value_keynames), sorted(dl.value_keynames))
            ts_keyname = [key for key in description if description[key]["coltype"] == "ts"][0]
            print("ts_keyname:", ts_keyname)
            self.assertEqual(ts_keyname, dl.ts_keyname)
            datatypes = dict([(key, description[key]["datatype"]) for key in description if description[key]["coltype"] == "value"])
            print("datatypes:", datatypes)
            self.assertEqual(datatypes, dl.datatypes)
            blacklist = tuple([key for key in description if description[key]["coltype"] == "blacklist"])
            print("blacklist:", blacklist)
            self.assertEqual(datatypes, dl.datatypes)
            headers_unsorted = [(key, description[key]["colpos"]) for key in description if description[key]["colpos"] is not None]
            headers = tuple([item[0] for item in sorted(headers_unsorted, key=lambda item: item[1])])
            print("headers:", headers)
            self.assertEqual(headers, dl.headers)
            label_texts = dict([(key, description[key]["label_text"]) for key in description])
            print("label:", label_texts)
            label_units = dict([(key, description[key]["label_unit"]) for key in description])
            print("label units:", label_units)
        # dump yaml file"
        metadir = os.path.join(dl.basedir, dl.project, "meta")
        metafile = os.path.join(metadir, "%s.yaml" % dl.tablename)
        if not os.path.isfile(metafile):
            meta = {
                "interval" : dl.interval,
                "descriptions": {},
                "delimiter" : dl.delimiter,
            }
            description = meta["description"]
            for colpos, header in enumerate(dl.headers):
                if header in dl.value_keynames:
                    coltype = "value"
                elif header in dl.index_keynames:
                    coltype = "index"
                elif header == dl.ts_keyname:
                    coltype = "ts"
                elif header in dl.blacklist:
                    coltype = "blacklist"
                else:
                    coltype = "unknown"
                if header in dl.datatypes:
                    datatype = dl.datatypes[header]
                else:
                    datatype = None
                description[header] = {
                    "colpos": colpos,
                    "coltype": coltype,
                    "datatype": datatype,
                    "label_text": "some text to show as label text",
                    "label_unit": "something/s"
                }
            print(yaml.dump(meta))
            print("writing %s" % metafile)
            with open(metafile, "wt") as outfile:
                outfile.write(yaml.dump(meta))

    def test_read_meta_new(self):
        dl = DataLogger("testdata")
        dl.setup("mysql", "performance", "2018-04-01")
        metadir = os.path.join(dl.basedir, dl.project, "meta") 
        metafile = os.path.join(metadir, "%s.yaml" % dl.tablename)
        if os.path.isfile(metafile):
            print("loading yaml style file %s", metafile)
            with open(metafile, "rt") as infile:
                meta = yaml.load(infile) 
            self.assertEqual(meta["interval"], dl.interval)
            self.assertEqual(meta["delimiter"], dl.delimiter)
            description = meta["descriptions"]
            index_keynames = tuple([key for key in description if description[key]["coltype"] == "index"])
            print("index_keynames:", index_keynames)
            self.assertEqual(index_keynames, dl.index_keynames)
            value_keynames = tuple([key for key in description if description[key]["coltype"] == "value"])
            print("value_kenames:", value_keynames)
            self.assertEqual(sorted(value_keynames), sorted(dl.value_keynames))
            ts_keyname = [key for key in description if description[key]["coltype"] == "ts"][0]
            print("ts_keyname:", ts_keyname)
            self.assertEqual(ts_keyname, dl.ts_keyname)
            datatypes = dict([(key, description[key]["datatype"]) for key in description if description[key]["coltype"] == "value"])
            print("datatypes:", datatypes)
            self.assertEqual(datatypes, dl.datatypes)
            blacklist = tuple([key for key in description if description[key]["coltype"] == "blacklist"])
            print("blacklist:", blacklist)
            self.assertEqual(datatypes, dl.datatypes)
            headers_unsorted = [(key, description[key]["colpos"]) for key in description if description[key]["colpos"] is not None]
            headers = tuple([item[0] for item in sorted(headers_unsorted, key=lambda item: item[1])])
            print("headers:", headers)
            self.assertEqual(headers, dl.headers)
            label_texts = dict([(key, description[key]["label_text"]) for key in description])
            print("label:", label_texts)
            label_units = dict([(key, description[key]["label_unit"]) for key in description])
            print("label units:", label_units)
        else:
            print("new yaml config file %s not found" % metafile)

    def test_convert(self):
        """
        load old style data, and dump new yaml file
        """
        dl = DataLogger("testdata")
        dl.setup("sanportperf", "fcIfC3AccountingTable", "2018-04-01")
        metadir = os.path.join(dl.basedir, dl.project, "meta")
        metafile = os.path.join(metadir, "%s.yaml" % dl.tablename)
        meta = {
            "interval" : dl.interval,
            "description": {},
            "delimiter" : dl.delimiter,
        }
        description = meta["description"]
        for colpos, header in enumerate(dl.headers):
            if header in dl.value_keynames:
                coltype = "value"
            elif header in dl.index_keynames:
                coltype = "index"
            elif header == dl.ts_keyname:
                coltype = "ts"
            elif header in dl.blacklist:
                coltype = "blacklist"
            else:
                coltype = "unknown"
            if header in dl.datatypes:
                datatype = dl.datatypes[header]
            else:
                datatype = None
            description[header] = {
                "colpos": colpos,
                "coltype": coltype,
                "datatype": datatype,
                "label_text": "some text to show as label text",
                "label_unit": "something/s"
            }
        print(yaml.dump(meta))
        if not os.path.isfile(metafile):
            print("writing %s" % metafile)
            with open(metafile, "wt") as outfile:
                outfile.write(yaml.dump(meta))

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
