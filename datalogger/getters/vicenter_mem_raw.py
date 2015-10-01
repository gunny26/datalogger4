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
    find correct value set in resultset
    """
    values = [0L] * len(result.sampleInfo)
    if len(result.value) == 1:
        values = result.value[0].value
    else:
        #logging.debug("Found more than one MetricSeries, searching for default series")
        # if there are more than one MetricSeries,
        # search for set with instance = '' and use this one
        # if there is no dfault set, buld average of available
        # datasets
        for metric_series in result.value:
            if metric_series.id.instance == '':
                values = metric_series.value
                break
            else:
                for index in range(len(metric_series.value)):
                    values[index] += metric_series.value[index]
    return(values)

def parse_result(result, data, name, counter):
    """
    pase data returned from performance coutner query
    to get desired data fromat
    """
    # first find correct values in result
    values = find_values(result)
    # get data in shape
    index = 0
    for sample_info in result.sampleInfo:
        timestamp = datetime_to_timestamp(sample_info.timestamp) # to unix timestamp
        if timestamp not in data:
            data[timestamp] = {
                name : {}
            }
        elif name not in data[timestamp]:
            data[timestamp][name] = {}
        data[timestamp][name][counter] = values[index] # index corresponds to index in results
        index += 1

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
            if mor['runtime.powerState'] == "poweredOn":
                if not ".tilak" in mor["moref"].name:
                    logging.debug("skipping non valid DNS virtual machine name %s", mor["moref"].name)
                    continue
                else:
                    logging.info("getting performance counter for %s", mor["moref"].name)
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
    for timestamp in sorted(data.keys()):
        for name in sorted(data[timestamp].keys()):
            if headers is True:
                outfile.write("ts\thostname\t%s\n" % "\t".join(sorted(data[timestamp][name].keys())))
                headers = False
            outfile.write("%s\t%s\t%s\n" % (timestamp, name, "\t".join((str(data[timestamp][name][key]) for key in sorted(data[timestamp][name].keys())))))
            lines += 1
    logging.info("got %d datasets", lines)
    logging.info("got %d timestamps", len(data.keys()))
    logging.info("got %d virtual machines", len(data[data.keys()[-1]].keys()))
    first = datetime.datetime.fromtimestamp(sorted(data.keys())[0])
    last = datetime.datetime.fromtimestamp(sorted(data.keys())[-1])
    logging.info("timerange from %s to %s", first, last)
    outfile.close()

# Start program
def main():
    """main what else"""
    #import cProfile
    #cProfile.run("main()")
    counters = (
        "mem.consumed.average",
        "mem.overhead.average",
        "mem.active.average",
        "mem.shared.average",
        "mem.granted.average",
        "mem.swapped.average",
        )
    basedir = "/var/rrd"
    project = "vicenter"
    tablename = "virtualMachineMemoryStats"
    interval = 35 # get 35 minutes of performance counters
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
    # save data
    save_data(basedir_raw, tablename, data)

if __name__ == "__main__":
    tvsp = tilak_vimomi.TilakVsphere()
    main()
