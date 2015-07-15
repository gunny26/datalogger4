#!/usr/bin/python
import datetime
import time
import sys
import os
import cProfile
import logging
logging.basicConfig(level=logging.DEBUG)
import tilak_wiki
from tilak_datalogger import Timeseries as Timeseries
from tilak_datalogger import TimeseriesArray as TimeseriesArray
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import DataLoggerHelper as dh

def vm_datastore(datalogger, start_ts=None, stop_ts=None):
    ts_field = "ts" # column name of timestamp information
    stat_func = "avg" # how to agregate timeseries data over time
    delim = "\t" # delimiter used in raw data
    # ['hostname', 'ts', 'vrsrdAdapterNo', 'vrsrdChannelNo',
    # 'vrsrdFirstSector', 'vrsrdFirstSector64', 'vrsrdKBRead',
    # 'vrsrdKBRead64', 'vrsrdKBWritten', 'vrsrdKBWritten64',
    # 'vrsrdLastSector', 'vrsrdLastSector64', 'vrsrdLun',
    # 'vrsrdOtherSCSICmd', 'vrsrdReadError', 'vrsrdSCSIReadCmd',
    # 'vrsrdSCSIWriteCmd', 'vrsrdScsiID', 'vrsrdType',
    # 'vrsrdVirResourceID', 'vrsrdWriteError']
    keys = ("hostname", 'vrsrdAdapterNo', 'vrsrdChannelNo', 'vrsrdScsiID', 'vrsrdLun') # unique key fields of raw data
    blacklist_keynames = ('vrsrdFirstSector', 'vrsrdFirstSector64', 'vrsrdKBRead', 'vrsrdKBWritten', 'vrsrdLastSector', 'vrsrdLastSector64', 'vrsrdType', 'vrsrdVirResourceID')
    starttime = time.time()
    tsa = dh.read_python(datalogger, ts_keyname=ts_field, index_keynames=keys, start_ts=start_ts, stop_ts=stop_ts, blacklist_keynames=blacklist_keynames)
    tsa.add_derive_col('vrsrdKBRead64', 'vrsrdKBRead64_d')
    tsa.add_derive_col('vrsrdKBWritten64', 'vrsrdKBWritten64_d')
    tsa.add_derive_col('vrsrdSCSIReadCmd', 'vrsrdSCSIReadCmd_d')
    tsa.add_derive_col('vrsrdSCSIWriteCmd', 'vrsrdSCSIWriteCmd_d')
    tsa.remove_col('vrsrdKBRead64')
    tsa.remove_col('vrsrdKBWritten64')
    tsa.remove_col('vrsrdSCSIReadCmd')
    tsa.remove_col('vrsrdSCSIWriteCmd')
    ws.send("Systembetrieb", datalogger.get_tablename(), wikitext)

def datetime_to_ts(datetime_object):
    return(int((datetime_object - datetime.datetime(1970, 1, 1)).total_seconds()))

def main():
    basedir = "/var/rrd/"
    project = "ipstor"
    tablename = "vrsRealDevTable"
    start = datetime.datetime(2015, 5, 19, 0, 0, 0)
    start_ts = datetime_to_ts(start)
    print start_ts, start
    stop = datetime.datetime(2015, 5, 19, 23, 59, 0)
    stop_ts = datetime_to_ts(stop)
    print stop_ts, stop
    datalogger = DataLogger(basedir, project, tablename)
    vm_datastore(datalogger, start_ts, stop_ts)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
