#!/usr/bin/python

import json
import gzip
import os
import time
import datalogger
import cProfile
import logging
logging.basicConfig(level=logging.DEBUG)

basedir = "/var/rrd/"
project = "vicenter"
tablename = "hostSystemDiskStats"

def test_baseline():
# reqading really raw data, this should be the best time
    starttime = time.time()
    rowcount = 0
    valuecount = 0
    data = json.load(open("/var/rrd/global_cache/2015-08-09/vicenter/hostSystemDiskStats/tsa_KHUnaG9zdG5hbWUnLCB1J2luc3RhbmNlJyk=.json"))
    print data.keys()
    for filename in data["ts_filenames"]:
        print "reading %s" % filename
        for line in gzip.open(os.path.join("/var/rrd/global_cache/2015-08-09/vicenter/hostSystemDiskStats/", filename)):
            cols = line.split(";")
            rowcount += 1
            valuecount += len(cols)
    print "Duration %s" % (time.time() - starttime)
    print "read %s rows" % rowcount
    print "read %s values" % valuecount

def test_baseline_float():
# reqading really raw data, this should be the best time
    starttime = time.time()
    rowcount = 0
    valuecount = 0
    data = json.load(open("/var/rrd/global_cache/2015-08-09/vicenter/hostSystemDiskStats/tsa_KHUnaG9zdG5hbWUnLCB1J2luc3RhbmNlJyk=.json"))
    print data.keys()
    for filename in data["ts_filenames"]:
        print "reading %s" % filename
        data = gzip.open(os.path.join("/var/rrd/global_cache/2015-08-09/vicenter/hostSystemDiskStats/", filename)).read().split("\n")
        for line in data[1:]:
            cols = line.split(";")
            cols = [float(value) for value in cols]
            rowcount += 1
            valuecount += len(cols)
    print "Duration %s" % (time.time() - starttime)
    print "read %s rows" % rowcount
    print "read %s values" % valuecount

def test_load_tsa():
    dl = datalogger.DataLogger(basedir, project, tablename)
    starttime = time.time()
    rowcount = 0
    tsa = dl.load_tsa("2015-08-09")
    for index_key in tsa.keys():
        print index_key, len(tsa[index_key])
        rowcount += len(tsa[index_key])
    print "Duration %s" % (time.time() - starttime)
    print "read %s rows" % rowcount

def test_load_tsastats():
    dl = datalogger.DataLogger(basedir, project, tablename)
    starttime = time.time()
    tsastats = dl.load_tsastats("2015-08-09")
    for index_key, tsstat in tsastats.items():
        print index_key
        for value_key in tsstat.keys():
            print value_key, tsstat[value_key]["max"]
    print "Duration %s" % (time.time() - starttime)

def test_load_quantile():
    if os.path.isfile("/var/rrd/global_cache/2015-08-09/vicenter/hostSystemDiskStats/quantile.json"):
        os.unlink("/var/rrd/global_cache/2015-08-09/vicenter/hostSystemDiskStats/quantile.json")
    dl = datalogger.DataLogger(basedir, project, tablename)
    starttime = time.time()
    rowcount = 0
    tsa = dl.load_tsa("2015-08-09")
    tsa.cache = True # this  improves calculation speed dramaticaly, but alos memory usage
    tsastats = dl.load_tsastats("2015-08-09")
    qa = datalogger.QuantileArray(tsa, tsastats)
    #qa = dl.load_quantile("2015-08-09")
    print qa
    print "Duration %s" % (time.time() - starttime)
    print "read %s rows" % rowcount


cProfile.run("test_load_quantile()")
