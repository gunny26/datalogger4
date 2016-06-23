#!/usr/bin/env python

"""
Python program that generates various statistics for one or more virtual machines

A list of virtual machines can be provided as a comma separated list.
"""

from __future__ import print_function
from pyVmomi import vmodl, vim
#from pyVmomi import vmodl
import datetime
import logging
logging.basicConfig(level=logging.ERROR)
import os
# own classes
import tilak_vimomi


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
    extract values from resultset

    returns
    <dict>[instance] = <tuple>values
    """
    values = {}
    for metric_series in result.value:
        instance = metric_series.id.instance
        values[instance] = metric_series.value
    return values

def parse_result(result, data, name, counter):
    """
    pase data returned from performance coutner query
    to get desired data fromat
    """
    # first find correct values in result
    values = find_values(result)
    # get data in shape
    for index, sample_info in enumerate(result.sampleInfo):
        # iterating over every timestamp in sampleInfo
        timestamp = datetime_to_timestamp(sample_info.timestamp) # to unix timestamp
        for instance in values.keys():
            # if there is only one instance - use it,
            # if there are multiple, ignore the instance named ""
            if (len(values.keys()) >  1) and (instance == ""):
                continue
            # build keyname, and correct "" to "all"
            # memory will be only this instance name
            keyid = (timestamp, name, instance)
            if instance == "":
                keyid = (timestamp, name, "all")
            if keyid not in data:
                data[keyid] = {
                    counter : values[instance][index] # index corresponds to index in results
                }
            else:
                data[keyid][counter] = values[instance][index]
    return data

def get_perf_values(mor, vchtime, interval, perf_dict, data, counters):
    """
    get performance counters for this mor
    """
    for counter in counters:
        resultset = tvsp.build_query(vchtime, perf_dict[counter], "*", mor, interval)
        if resultset is not None:
            parse_result(resultset[0], data, mor.name, counter)
        else:
            raise EmptyResultSet("%s no performance data received" % mor.name)
    return(data)

def get_data(managed_object, interval, counters):
    """
    get data from vicenter
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
            #if mor["moref"].name != "WS00008220":
            #    continue
            if mor['runtime.powerState'] == "poweredOn":
                try:
                    get_perf_values(mor['moref'], vchtime, interval, perf_dict, data, counters)
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

def save_data(basedir_raw, tablename, data):
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
    lines = 0
    for keyid in sorted(data.keys()):
        timestamp, name, instance = keyid
        if headers is True:
            outfile.write("ts\thostname\tinstance\t%s\n" % "\t".join(sorted(data[keyid].keys())))
            headers = False
        outfile.write("%s\t%s\t%s\t%s\n" % (timestamp, name, instance, "\t".join((str(data[keyid][counter]) for counter in sorted(data[keyid].keys())))))
        lines += 1
    logging.info("got %d datasets", lines)
    logging.info("got %d unique keys", len(data.keys()))
    first = datetime.datetime.fromtimestamp(sorted(data.keys())[0][0])
    last = datetime.datetime.fromtimestamp(sorted(data.keys())[-1][0])
    logging.info("timerange from %s to %s", first, last)
    outfile.close()

# Start program
def main():
    """main what else"""
    #import cProfile
    #cProfile.run("main()")
    counter_dict = {
        "virtualMachineCpuStats" : ( # datalogger tablename
            "cpu.idle.summation", # vicenter counter names in this table
            "cpu.ready.summation",
            # "cpu.system.summation", vicenter 6 delivers this counter
            # only with all instance, so not mixable with the other
            # counters
            "cpu.used.summation",
            "cpu.wait.summation",
            ),
        "virtualMachineMemoryStats": (
            "mem.consumed.average",
            "mem.overhead.average",
            "mem.active.average",
            "mem.shared.average",
            "mem.granted.average",
            "mem.swapped.average",
            ),
        "virtualMachineDatastoreStats" : (
            "datastore.totalReadLatency.average",
            "datastore.totalWriteLatency.average",
            "datastore.read.average",
            "datastore.write.average",
            "datastore.numberReadAveraged.average",
            "datastore.numberWriteAveraged.average",
            ),
        "virtualMachineDiskStats" : (
            "disk.numberReadAveraged.average",
            "disk.numberWriteAveraged.average",
            "disk.read.average",
            "disk.write.average",
            "disk.commandsAborted.summation",
            "disk.commands.summation",
            "disk.busResets.summation",
            ),
        "virtualMachinePowerStats" : (
            "power.power.average",
            ),
        "virtualMachineNetworkStats" : (
            "net.usage.average",
            "net.received.average",
            "net.transmitted.average",
            "net.droppedRx.summation",
            "net.droppedTx.summation",
            ),
    }
    basedir = "/var/rrd"
    project = "vdi6"
    interval = 35 # get 35 minutes of performance counters
    for tablename, counters in counter_dict.items():
        logging.info("fetching table %s", tablename)
        # create basic directory structure
        if not os.path.exists(os.path.join(basedir, project)):
            os.mkdir(os.path.join(basedir, project))
        basedir_raw = os.path.join(basedir, project, "raw")
        basedir_rrd = os.path.join(basedir, project, "rrd")
        basedir_gfx = os.path.join(basedir, project, "gfx")
        for directory in (basedir_raw, basedir_rrd, basedir_gfx):
            if not os.path.exists(directory):
                os.mkdir(directory)
        # get data
        data = get_data(vim.VirtualMachine, interval, counters)
        if len(data) > 0:
            logging.info("saving data")
            # save data
            save_data(basedir_raw, tablename, data)
        else:
            logging.info("no data receiveed")

if __name__ == "__main__":
    tvsp = tilak_vimomi.TilakVdi6()
    main()
