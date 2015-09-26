#!/usr/bin/python

import datetime
import time
import cPickle as cPickle
import json
import os
import base64
import gzip
import hashlib
import logging
# own modules
from TimeseriesArray import TimeseriesArray as TimeseriesArray

def read_python(datalogger, ts_keyname, index_keynames, start_ts=None, stop_ts=None, blacklist_keynames=None):
    """
    read data from file and convert every row into dict and add this data to Timeseries Object
    header names must be present in first row of file
    """
    headers = datalogger.get_headers()
    # add all non-index_keynames and non-ts_keyname to values keys
    value_keynames = []
    for key in headers:
        if (key not in index_keynames) and (key != ts_keyname):
            value_keynames.append(key)
    # remove blacklisted keys, if given
    if blacklist_keynames is not None:
        for key in blacklist_keynames:
            assert key in value_keynames
            value_keynames.remove(key)
    tsa = TimeseriesArray(index_keynames, value_keynames)
    for rowdict in datalogger.get_dict_data(start_ts, stop_ts):
        try:
            tsa.add(rowdict)
        except AssertionError as exc:
            print rowdict
            raise exc
    return(tsa)

def read_row(start_ts, stop_ts):
    """
    generator to return one parsed line from raw files where
    ts timestamp matches between start_ts and stop_ts

    parameters:
    start_ts <int> starting timestamp
    stop_ts <int> stopping timestamp

    yields:
    <dict> of every row, where timestamp value ist in between start_ts and stop_ts
    """
    for filename in self.get_filenames(start_ts, stop_ts):
        assert start_ts < stop_ts
        for lineno, row in enumerate(self.__get_file_handle(filename, "rb")):
            if row.startswith(self.headers[0]): # skip header line
                continue
            try:
                data = self.__parse_line(row.strip())
                if data[self.ts_keyname] > stop_ts: # stop if data is above stop_ts
                    return
                elif data[self.ts_keyname] < start_ts: # got further if below start_ts
                    continue
                yield data
            except IndexError as exc:
                logging.exception(exc)
                logging.error("Format Error in File %s, line %s", filename, lineno)
                logging.error("Data: %s", row.strip())


def read_one(datalogger, datestring, blacklist_keynames=None):
    """
    read data from file and convert every row into dict and add this data to TimeseriesArray Object,
    which creates Timeseries Objects for every unique key

    this version, reads only one specific unique key, to safe memory and time.
    useful if you want only one specific Timeseries, probably to draw a graph
    """
    headers = datalogger.get_headers()
    ts_keyname = datalogger.get_ts_keyname()
    index_keynames = datalogger.get_indey_keynames()
    # add all non-index_keynames and non-ts_keyname to values keys
    value_keynames = []
    for key in headers:
        if (key not in index_keynames) and (key != ts_keyname):
            value_keynames.append(key)
    # remove blacklisted keys, if given
    if blacklist_keynames is not None:
        for key in blacklist_keynames:
            assert key in value_keynames
            value_keynames.remove(key)
    tsa = TimeseriesArray(index_keynames, value_keynames)
    for rowdict in self.read_row(datestring):
        # this beauty comes from this question
        # http://stackoverflow.com/questions/9323749/python-check-if-one-dictionary-is-a-subset-of-another-larger-dictionary
        if all((k in rowdict and rowdict[k]==v) for k,v in index_keynames) is True:
            #logging.debug("found corect key %s in row %s", index_dict, rowdict)
            tsa.add(rowdict)
    return(tsa)

def read_day(datalogger, ts_keyname, index_keynames, datestring, blacklist_keynames=None, use_cache=True):
    """
    read data from file and convert every row into dict and add this data to Timeseries Object
    header names must be present in first row of file

    similar to read_timestamp, but this function reads only one specific file

    datalogger <DataLogger> object to handle some DataLogger things
    ts_keyname <str> the name of the timestamp column in raw data
    index_keynames <tuple of str> names of index keys in raw data
    day <str> isodate representation of date like 2015-12-31
    blacklistr_keynames <tuple of str> columnnames to ignore from raw data
    use_cache <bool> use cached TimeseriesArray object or read from raw data
    """
    md5_key = hashlib.md5()
    argstring = "%s,%s,%s,%s" % (ts_keyname, index_keynames, datestring, blacklist_keynames)
    md5_key.update(argstring)
    logging.debug("created hexdigest from arguments %s is %s", argstring, md5_key.hexdigest())
    cachefile = os.path.join(datalogger.cache_basedir, "tsa_%s_%s_%s.dmp" % (datalogger.get_project(), datalogger.get_tablename(), md5_key.hexdigest()))
    tsa = None
    if os.path.isfile(cachefile) and use_cache is True:
        starttime = time.time()
        logging.debug("loading stored tsa object file %s", cachefile)
        tsa = cPickle.load(open(cachefile, "rb"))
        logging.debug("loaded stored tsa data in %s seconds", time.time() - starttime)
    else:
        starttime = time.time()
        # add all non-index_keynames and non-ts_keyname to values keys
        value_keynames = get_value_keynames(ts_keyname, index_keynames, datalogger.get_headers(), blacklist_keynames)
        tsa = TimeseriesArray(index_keynames, value_keynames)
        for rowdict in datalogger.get_dict_data_datestring(datestring):
            try:
                tsa.add(rowdict)
            except AssertionError as exc:
                print rowdict
                raise exc
        logging.debug("Read raw data file in %s seconds", time.time() - starttime)
        if use_cache is True:
            logging.debug("dumping tsa object data to %s", cachefile)
            starttime = time.time()
            cPickle.dump(tsa, open(cachefile, "wb"), cPickle.HIGHEST_PROTOCOL)
            logging.debug("dumped tsa data in %s seconds", time.time() - starttime)
        metafile = os.path.join(datalogger.meta_basedir, "%s.json" % datalogger.get_tablename())
        if not os.path.isfile(metafile):
            meta = {
                "ts_keyname" : ts_keyname,
                "index_keynames" : index_keynames,
                "value_keynames" : value_keynames,
                "headers" : datalogger.get_headers(),
                "blacklist" : blacklist_keynames,
                "delimiter" : datalogger.get_delimiter(),
            }
            json.dump(meta, open(metafile, "wb"))
            logging.debug("dumped meta information in filename %s", metafile)
    return(tsa)

def get_value_keynames(ts_keyname, index_keynames, headers, blacklist_keynames=None):
    value_keynames = []
    for key in headers:
        if (key not in index_keynames) and (key != ts_keyname):
            value_keynames.append(key)
    # remove blacklisted keys, if given
    if blacklist_keynames is not None:
        for key in blacklist_keynames:
            assert key in value_keynames
            value_keynames.remove(key)
    return(value_keynames)

def save_tsa(datalogger, tsa):
    """
    datalogger <DataLogger>
    tsa <TimeseriesArray>

    save all Timeseries in TimeseriesArray with cPickle
    """
    project = datalogger.get_project()
    tablename = datalogger.get_tablename()
    start_ts = tsa.get_first_ts()
    stop_ts = tsa.get_last_ts()
    base_filename = "tsa_%s_%s_%d_%d" % (project, tablename, start_ts, stop_ts)
    logging.debug("using cache_basedir %s" % datalogger.cache_basedir)
    for keys in tsa.keys():
        filename = "%s_%s.dmp.gz" %  (base_filename, "_".join((base64.b32encode(key) for key in keys)))
        logging.debug("Storing Timeseries Data to %s" % filename)
        cPickle.dump(tsa[keys], gzip.open(os.path.join(datalogger.cache_basedir, filename), "wb"), cPickle.HIGHEST_PROTOCOL)

def load_tsa(datalogger, ts_keyname, index_keynames, start_ts, stop_ts):
    """
    datalogger <DataLogger>
    ts_keyname <str>
    index_keynames <tuple>
    start_ts <float>
    stop_ts <float>
    """
    base_filename = "tsa_%s_%s_%d_%d" % (datalogger.get_project(), datalogger.get_tablename(), start_ts, stop_ts)
    logging.debug("searching for pattern %s", base_filename)
    logging.debug("using cache_basedir %s" % datalogger.cache_basedir)
    value_keynames = get_value_keynames(ts_keyname, index_keynames, datalogger.get_headers())
    tsa = TimeseriesArray(index_keynames, value_keynames)
    for dumpfile in os.listdir(datalogger.cache_basedir):
        logging.debug("Found file %s", dumpfile)
        if not dumpfile.startswith(base_filename):
            continue
        logging.debug("Found matching file %s", dumpfile)
        name = dumpfile.split(".")[0]
        fields = name.split("_")
        project = fields[1]
        logging.debug("Found %s", project)
        tablename = fields[2]
        logging.debug("Found %s", tablename)
        start_ts = float(fields[3])
        logging.debug("Found %s", start_ts)
        stop_ts = float(fields[4])
        logging.debug("Found %s", stop_ts)
        encoded_keys = fields[5:]
        logging.debug("encoded keys %s", encoded_keys)
        keys = tuple((base64.b32decode(key) for key in encoded_keys))
        logging.debug("Found %s", keys)
        tsa.data[keys] = cPickle.load(gzip.open(os.path.join(datalogger.cache_basedir, dumpfile), "rb"))
    return(tsa)

def get_wiki_table(tsa, time_func="avg", value_func=lambda key, value_key, value : "%0.1f" % value):
    """
    tsa <TimeseriesArray> Object which hold data and statistics for many index_keys
    time_func <str> Function to aggregate Timeseries Data over Time

    returns foswiki string which represents a table
    """
    wikitext = ""
    firstrow = True
    index_keys = tsa.get_index_keys()
    for key in tsa.keys():
        if len(tsa[key]) == 0: # skip empty timeseries objects
            continue
        fields = sorted(tsa[key].get_headers()) # get exact this order of fields
        if firstrow is True:
            wikitext += "| " + " | ".join(("*%s*" % value for value in index_keys)) + " | " + " | ".join(("*%s*" % field for field in fields)) + " |\n"
            firstrow = False
        try:
            stat_dict = tsa[key].get_stat(time_func)
            wikitext += "| %s |  %s |\n" % (" | ".join(key), " |  ".join(("%s" % value_func(key, field, stat_dict[field]) for field in fields)))
        except StandardError as exc:
            logging.info(key)
            logging.exception(exc)
    return(wikitext)

def get_wiki_dict_table(data, keyfunc=lambda a: a):
    """
    data <dict> input data in dict form

    returns fowiki formatted string which represents a table
    """
    wikitext = ""
    firstrow = True
    for key in data:
        fields = sorted(data[key].keys()) # get exact this order of fields
        if firstrow is True:
            wikitext += "| *key* | "+ " | ".join(("*%s*" % field for field in fields)) + " |\n"
            firstrow = False
        wikitext += "| %s |  %s |\n" % (keyfunc(key), " |  ".join(("%0.1f" % data[key][field] for field in fields)))
    return(wikitext)

def datetime_to_ts(datetime_object):
    """
    return unix timestamp from given datetime object
    """
    return(int((datetime_object - datetime.datetime.fromtimestamp(0)).total_seconds()))

def get_ts_from_datestring(datestring):
    """
    datestring <str> in isodate format like 2015-12-31

    returns first <int> and last <int> timestamp of this date
    first -> 2015-12-31 00:00:00.0
    last -> 2015-12-31 23:59:59.999
    """
    year, month, day = (int(part) for part in datestring.split("-"))
    start = datetime.datetime(year, month, day, 0, 0, 0)
    start_ts = datetime_to_ts(start)
    stop = datetime.datetime(year, month, day, 23, 59, 59)
    stop_ts = datetime_to_ts(stop)
    return(start_ts + time.timezone, stop_ts + time.timezone)
