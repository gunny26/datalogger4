#!/usr/bin/env python

"""
Python program that generates various statistics for one or more virtual machines

A list of virtual machines can be provided as a comma separated list.
"""

from __future__ import print_function
from pyVmomi import vmodl, vim
#from pyVmomi import vmodl
import datetime
import time
import json
import logging
logging.basicConfig(level=logging.ERROR)
import os
# own classes
import tilak_vimomi


def timeit(func):
    def inner(*args, **kwds):
        starttime = time.time()
        ret = func(*args, **kwds)
        logging.info("Duration of call to %s : %f", func.__name__, time.time() - starttime)
        return ret
    return inner

class EmptyResultSet(Exception):
    """indicate empty resultset"""
    pass


def datetime_to_timestamp(in_datetime):
    """
    return unixtimestamp from datetime object
    """
    utc_naive  = in_datetime.replace(tzinfo=None) - in_datetime.utcoffset()
    timestamp = (utc_naive - datetime.datetime(1970, 1, 1)).total_seconds()
    return timestamp

def find_values(result):
    """
    find correct value set in resultset
    """
    values = {}
    for metric_series in result.value:
        instance = metric_series.id.instance
        #logging.debug("Found instance %s", instance)
        values[instance] = metric_series.value
    return(values)

def parse_result(result, data, name, counter, use_void_instance):
    """
    pase data returned from performance coutner query
    to get desired data fromat

    parameters:
    -----------
    data <object> as got from vicenter query
    name <str> additional information to build a key
    counter <str> name of this counter
    use_void_instance <bool> use instance without name "",
        useful for Memory, where only this instance exists
    """
    # first find correct values in result
    values = find_values(result)
    # get data in shape
    for index, sample_info in enumerate(result.sampleInfo):
        timestamp = datetime_to_timestamp(sample_info.timestamp) # to unix timestamp
        for instance in values.keys():
            # for some counters we have to use the void instance data,
            # for others this should be skipped
            keyid = (timestamp, name, instance)
            if (instance == "") and (use_void_instance is True):
                keyid = (timestamp, name)
            elif (instance == "") and (use_void_instance is False):
                continue # skip this
            if keyid not in data:
                #logging.debug("correct key %s", keyid)
                data[keyid] = {
                    counter : values[instance][index] # index corresponds to index in results
                }
            else:
                data[keyid][counter] = values[instance][index]

def get_perf_values(mor, vchtime, interval, perf_dict, data, meta_data):
    """
    get performance counters for this mor

    parameters:
    -----------
    vchtime <datetimte?> time of vicenter server
    interval <int> time interval to get from vicenter
    perf_dict <object> vicenter object to get performance counter from
    data <dict> object to hold received data
    counters <tuple> name of counters to fetch
    """
    for counter in meta_data["counters"]:
        resultset = tvsp.build_query(vchtime, perf_dict[counter], "*", mor, interval)
        if resultset is not None:
            if "instance" not in meta_data["index_keys"]:
                parse_result(resultset[0], data, mor.name, counter, use_void_instance=True)
            else:
                parse_result(resultset[0], data, mor.name, counter, use_void_instance=False)
        else:
            raise EmptyResultSet("%s no performance data received" % mor.name)
    return(data)

@timeit
def get_data(managed_object, interval, meta_data):
    """
    get data from vicenter and further pass this to get_perf_values to
    extract performance counter values per instance

    parameters:
    ----------
    managed_object <object> witch type of object to search for,
        ex. vim.HostSystem or vim.VirtualMachine
    interval <int> which time interval to fetch
    meta_data <dict> holds information about counters to fetch
    """
    try:
        data = {}
        # Get vCenter date and time for use as baseline when querying for counters
        vchtime = tvsp.si.CurrentTime()
        # Get all the performance counters
        perf_dict = tvsp.get_perf_dict()
        properties = tvsp.get_properties([managed_object], ['name', 'runtime.powerState'], managed_object)
        #Find VM supplied as arg and use Managed Object Reference (moref) for the PrintVmInfo
        for mor in properties:
            #if mor["moref"].name != "srvcacapp1.tilak.cc":
            #    continue
            if mor['runtime.powerState'] == "poweredOn":
                if not ".tilak" in mor["moref"].name:
                    logging.debug("skipping non valid DNS virtual machine name %s", mor["moref"].name)
                    continue
                else:
                    logging.info("getting performance counter for %s", mor["moref"].name)
                try:
                    get_perf_values(mor['moref'], vchtime, interval, perf_dict, data, meta_data)
                except EmptyResultSet as exc:
                    logging.exception(exc)
                    continue
            else:
                logging.debug('Problem connecting to Virtual Machine.  {} is likely powered off or suspended'.format(mor['name']))
        return(data)
    except vmodl.MethodFault as exc:
        logging.exception(exc)
    except Exception as exc:
        logging.exception(exc)

@timeit
def save_data(basedir_raw, tablename, meta_data, data):
    """
    save data in data to raw file with isoformat extention in basedir_raw
    """
    # output full dataset
    filename = os.path.join(basedir_raw, "%s_%s.csv" % (tablename, datetime.date.today().isoformat()))
    outfile = None
    headers = True
    if os.path.exists(filename):
        # open in append mode if file exists
        outfile = open(filename, "ab")
        # and print headers
        headers = False
    else:
        outfile = open(filename, "wb")
    for lines, keyid in enumerate(sorted(data.keys())):
        if headers is True:
            outfile.write("%s\t%s\t%s\n" % (meta_data["ts_keyname"], "\t".join(meta_data["index_keys"]), "\t".join(meta_data["counters"])))
            headers = False
        try:
            outfile.write("%s\t%s\n" % ("\t".join((str(key) for key in keyid)), "\t".join((str(data[keyid][counter]) for counter in meta_data["counters"]))))
        except KeyError as exc:
            logging.error("DataformatError skipping %s : %s", keyid, data[keyid])
    logging.info("got %d datasets", lines)
    logging.info("got %d unique keys", len(data.keys()))
    first = datetime.datetime.fromtimestamp(sorted(data.keys())[0][0])
    last = datetime.datetime.fromtimestamp(sorted(data.keys())[-1][0])
    logging.info("timerange from %s to %s", first, last)
    outfile.close()

@timeit
def save_meta(meta_basedir, tablename, meta_data):
    headers = ["ts", ]
    headers.extend(meta_data["index_keys"])
    headers.extend(meta_data["counters"])
    meta = {
        "ts_keyname" : "ts",
        "blacklist" : meta_data["blacklist"],
        "headers" : headers,
        "delimiter" : meta_data["delimiter"],
        "value_keynames" : meta_data["counters"],
        "index_keynames" : meta_data["index_keys"],
    }
    metafile = os.path.join(meta_basedir, "%s.json" % tablename)
    if not os.path.isfile(metafile):
        json.dump(meta, open(metafile, "wb"))

# Start program
def main():
    """main what else"""
    #import cProfile
    #cProfile.run("main()")
    basedir = "/var/rrd"
    project = "vicenter"
    interval = 35 # get 35 minutes of performance counters
    # create basic directory structure
    if not os.path.exists(os.path.join(basedir, project)):
        os.mkdir(os.path.join(basedir, project))
    basedir_raw = os.path.join(basedir, project, "raw")
    basedir_meta = os.path.join(basedir, project, "meta")
    basedir_rrd = os.path.join(basedir, project, "rrd")
    basedir_gfx = os.path.join(basedir, project, "gfx")
    for directory in (basedir_raw, basedir_rrd, basedir_gfx):
        if not os.path.exists(directory):
            os.mkdir(directory)
    # get data
    counters_dict = {
        "hostSystemDiskStats" : {
            "ts_keyname" : "ts",
            "index_keys" : (
                "hostname",
                "instance",
            ),
            "counters" : (
                "disk.kernelWriteLatency.average",
                "disk.kernelReadLatency.average",
                "disk.totalWriteLatency.average",
                "disk.totalReadLatency.average",
                "disk.deviceWriteLatency.average",
                "disk.deviceReadLatency.average",
                "disk.numberWrite.summation",
                "disk.numberRead.summation",
                "disk.write.average",
                "disk.read.average",
            ),
            "delimiter" : "\t",
            "blacklist" : (),
        },
       "hostSystemMemoryStats": {
            "ts_keyname" : "ts",
            "index_keys" : (
                "hostname",
            ),
            "counters" : (
                "mem.consumed.average",
                "mem.zero.average",
                "mem.active.average",
                "mem.vmmemctl.average",
                "mem.unreserved.average",
                "mem.swapused.average",
                "mem.heapfree.average",
                "mem.granted.average",
            ),
            "delimiter" : "\t",
            "blacklist" : (),
        },
        "hostSystemCpuStats" : {
            "ts_keyname" : "ts",
            "index_keys" : (
                "hostname",
                "instance",
            ),
            "counters" : (
                "cpu.idle.summation",
                "cpu.used.summation",
            ),
            "delimiter" : "\t",
            "blacklist" : (),
        },
        "hostSystemNetworkStats" : {
            "ts_keyname" : "ts",
            "index_keys" : (
                "hostname",
                "instance",
            ),
            "counters" : (
                "net.usage.average",
                "net.received.average",
                "net.transmitted.average",
                "net.droppedRx.summation",
                "net.droppedTx.summation",
            ),
            "delimiter" : "\t",
            "blacklist" : (),
        },
    }
    for tablename, meta_data in counters_dict.items():
        logging.info("Getting counters for tablename %s", tablename)
        data = get_data(vim.HostSystem, interval, meta_data)
        save_data(basedir_raw, tablename, meta_data, data)
        save_meta(basedir_meta, tablename, meta_data)

if __name__ == "__main__":
    tvsp = tilak_vimomi.TilakVsphere()
    main()
