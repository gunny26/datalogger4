#!/usr/bin/python3
"""
fast functions to read from raw input csv files and create TimeseriesArray/Timeseries Structures

csv -> TSA (definiton of many Timeseries) -> TS1
                                          -> TS2
                                          -> ...
"""
import os
import gzip
import json
import time
import concurrent.futures
import logging
# own modules
from datalogger4.b64 import b64encode

def get_ts_writer(ts_keyname, value_keynames):
    """
    prepare function to write single timeseries to file

    :param ts_keyname <str>: dict key of timstamp column
    :param value_keynames <list>: list of value_keynames to store from dict
    """
    def ts_writer(filename, data):
        """
        write data to file, timestamp by timestamp
        no buffer, lower memory consumption

        :param filename <str>: name of output file
        :param data <dict>: data of this Timeseries
        """
        firstrow = True
        with gzip.open(filename, "wt") as outfile:
            for ts in sorted(data.keys()):
                if firstrow:
                    outfile.write(";".join([ts_keyname,] + value_keynames) + "\n")
                    firstrow = False
                outfile.write(";".join([str(ts), ] + [str(value) for value in data[ts]]) + "\n")
    return ts_writer

def get_ts_buffer_writer(ts_keyname, value_keynames):
    """
    prepare function to write single timeseries to file

    :param ts_keyname <str>: dict key of timstamp column
    :param value_keynames <list>: list of value_keynames to store from dict
    """
    def ts_buffer_writer(filename, data):
        """
        write data to file, buffered version
        first prepare data in list, afterwards write it to disk at once

        :param filename <str>: name of output file
        :param data <dict>: data of this Timeseries
        """
        outbuffer = []
        firstrow = True
        for ts in sorted(data.keys()):
            if firstrow:
                outbuffer.append(";".join([ts_keyname,] + value_keynames))
                firstrow = False
            outbuffer.append(";".join([str(ts), ] + [str(value) for value in data[ts]]))
        with gzip.open(filename, "wt") as outfile:
            outfile.write("\n".join(outbuffer))
    return ts_buffer_writer

def get_raw_reader(filename, delimiter, headers, tsa_def):
    """
    prepare reading of raw or archived (.gz) file and return
    function to read whole file in once

    low memory funtion, read line by line

    :param filename <str>: name of input file
    :param delimiter <char>: delimiting character
    :param headers <list>: list of header columns - order matters
    :param tsa_def <dict>: table definition
    """
    index_keynames = tsa_def["index_keys"]
    value_keynames = tsa_def["value_keys"]
    ts_keyname = tsa_def["ts_key"]
    if filename.endswith(".gz"):
        fh = gzip.open(filename, "rt")
    else:
        fh = open(filename, "rt")
    def inner():
        tsa = {}
        skipped = 0
        done = 0
        with fh as infile:
            firstrow = True
            fileheaders = None
            for row in infile: # row is already stripped
                if not row:
                    continue
                elif firstrow:
                    fileheaders = row.strip().split(delimiter)
                    firstrow = False
                    if fileheaders != headers:
                        logging.error("defined headers       : %s", headers)
                        logging.error("headers in input file : %s", fileheaders)
                        logging.error("header not defined    : %s", [header for header in headers if header not in fileheaders])
                        logging.error("header missing in file: %s", [header for header in fileheaders if header not in headers])
                        # defined headers in meta file are master,
                        # headers line in file is additional
                        # raise AssertionError("Header mismatch")
                else:
                    try:
                        cols = row.strip().split(delimiter)
                        if len(cols) != len(headers):
                            logging.debug("number of columns in line %d does not match defined headers %d", len(cols), len(headers))
                            logging.debug(cols)
                            skipped += 1
                            continue
                        data_dict = dict(zip(headers, cols)) # row_dict
                        ts = float(data_dict[ts_keyname])
                        key = tuple([data_dict[index_key] for index_key in index_keynames])
                        values = [float(data_dict[value_key]) for value_key in value_keynames]
                        if key not in tsa:
                            tsa[key] = {}
                        tsa[key][ts] = values
                        # print(key, ts, values)
                        done += 1
                    except ValueError as exc:
                        # if there is any error in converting input data to float, skip this line
                        skipped += 1
                    except KeyError as exc:
                        # if there is any key missing in row, kip this line
                        # for example in vdi6 virtualMachineNetworkStats there are some filed missing
                        logging.debug("missing key %s in data_dict %s", exc, data_dict)
                        logging.debug("raw line : %s", row.strip().split(delimiter))
                        skipped += 1
        logging.info("done %d lines, skipped %d lines", done, skipped)
        return tsa
    return inner

def get_raw_buffer_reader(filename, delimiter, headers, tsa_def):
    """
    prepare reading of raw or archived (.gz) file and return
    function to read whole file in once
    read whole file at once into memory

    :param filename <str>: name of input file
    :param delimiter <char>: delimiting character
    :param headers <list>: list of header columns - order matters
    :param tsa_def <dict>: table definition
    """
    index_keynames = tsa_def["index_keys"]
    value_keynames = tsa_def["value_keys"]
    ts_keyname = tsa_def["ts_key"]
    if filename.endswith(".gz"):
        fh = gzip.open(filename, "rt")
    else:
        fh = open(filename, "rt")
    def inner():
        """
        read input file and fill tsa structure with data
        """
        tsa = {}
        skipped = 0
        done = 0
        with fh as infile:
            firstrow = True
            fileheaders = None
            for row in infile.read().split("\n"):
                if not row:
                    continue
                elif firstrow:
                    fileheaders = row.strip().split(delimiter)
                    firstrow = False
                    if fileheaders != headers:
                        logging.error("defined headers       : %s", headers)
                        logging.error("headers in input file : %s", fileheaders)
                        logging.error("headers not found     : %s", [header for header in headers if header not in fileheaders])
                        logging.error("headers not defined   : %s", [header for header in fileheaders if header not in headers])
                        # defined headers in meta file are master,
                        # headers line in file is additional
                        # raise AssertionError("Header mismatch")
                else:
                    try:
                        cols = row.strip().split(delimiter)
                        assert len(cols) == len(headers)
                        data_dict = dict(zip(headers, cols)) # row_dict
                        ts = float(data_dict[ts_keyname])
                        key = tuple([data_dict[index_key] for index_key in index_keynames])
                        values = [float(data_dict[value_key]) for value_key in value_keynames]
                        if key not in tsa:
                            tsa[key] = {}
                        tsa[key][ts] = values
                        # print(key, ts, values)
                        done += 1
                    except ValueError as exc:
                        # if there is any error in converting input data to float, skip this line
                        skipped += 1
                    except KeyError as exc:
                        # if there is any key missing in row, kip this line
                        # for example in vdi6 virtualMachineNetworkStats there are some filed missing
                        logging.debug("missing key %s in data_dict %s", exc, data_dict)
                        logging.debug("raw line : %s", row)
                        skipped += 1
        logging.info("done %d lines, skipped %d lines", done, skipped)
        return tsa
    return inner

def fast_tsa(dl, max_workers=6):
    """
    import data from raw data analyze data and store it
    Datalogger object must be initialized to some project/tablename/datestring combination

    :param dl <Datalogger>: object to use for configuration
    """
    # define new tsa structure
    tsa_def = {
        "index_keys": list(dl.meta["index_keynames"]),
        "ts_filenames": [],
        "ts_key": dl.meta["ts_keyname"],
        "value_keys": list(dl.meta["value_keynames"])
    }
    # read input data
    starttime = time.time()
    logging.debug("starting read")
    # first bet, there is a *.csv.gz version
    if os.path.isfile(dl.archive_filename):
        logging.info("found archived raw input file %s", dl.archive_filename)
        raw_reader = get_raw_reader(dl.archive_filename, dl.meta["delimiter"], list(dl.meta["headers"]), tsa_def)
    # otherwise a csv (uncompressed) version
    elif dl.raw_filename is not None:
        logging.info("searching original raw input file %s", dl.raw_filename)
        # this will raise Exception if fil does not exist
        raw_reader = get_raw_reader(dl.raw_filename, dl.meta["delimiter"], list(dl.meta["headers"]), tsa_def)
    else:
        logging.error("neither archived nor raw file is available")
        return
    tsa = raw_reader() # read data into memory
    logging.debug("done in %0.2f", time.time() - starttime)
    starttime = time.time()
    logging.debug("dumping individual TS files")
    # output TS data
    ts_writer = get_ts_buffer_writer(tsa_def["ts_key"], tsa_def["value_keys"])
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for key in tsa: # split into Timeseries by index_key
            ts_filename = "ts_" + b64encode(key) + ".csv.gz"
            filename = os.path.join(dl.cachedir, ts_filename)
            tsa_def["ts_filenames"].append(ts_filename)
            futures.append(executor.submit(ts_writer, filename, tsa[key])) # build up queue
        for future in concurrent.futures.as_completed(futures): # wait for futures to complete
            future.result()
    # output tsa structure
    logging.debug("done in in %0.2f", time.time() - starttime)
    tsa_filename = "tsa_" + b64encode(tsa_def["index_keys"]) + ".json"
    logging.debug("dumping tsa to %s", tsa_filename)
    json.dump(tsa_def, open(os.path.join(dl.cachedir, tsa_filename), "wt"), indent=4) # dump tsa data
    logging.debug("done in in %0.2f", time.time() - starttime)
