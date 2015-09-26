#!/usr/bin/python

import web
import os
import logging
logging.basicConfig(level=logging.INFO)
import logging.handlers
import hashlib
import json
import datetime
import time
import rrdtool
import traceback
import cPickle
import gzip
import base64
from string import Template
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import DataLoggerHelper as dh
from tilak_datalogger import TimeseriesArray as TimeseriesArray

urls = (
    "/RawData/(.*)", "RawData",
    )

# add wsgi functionality
project = "vicenter"
basedir = "/var/rrd"
basedir_templates = os.path.join(basedir, "templates")
handler = logging.handlers.RotatingFileHandler(
    os.path.join(basedir, "datalogger.log"),
    maxBytes=10 * 1024 * 1024,
    backupCount=5)
logging.getLogger("").addHandler(handler)
logging.getLogger("").setLevel(level=logging.DEBUG)

def calllogger(func):
    """
    decorator
    """
    def inner(*args, **kwds):
        starttime = time.time()
        call_str = "%s(%s, %s)" % (func.__name__, args, kwds)
        logging.debug("calling %s", call_str)
        try:
            ret_val = func(*args, **kwds)
            logging.debug("duration of call %s : %s", call_str, (time.time() - starttime))
            return ret_val
        except StandardError as exc:
            logging.exception(exc)
            logging.error("call to %s caused StandardError", call_str)
            return("call to %s caused StandardError" % call_str)
    return inner

class RawData(object):
    """retrieve Data from RRD Archive"""

    def __init__(self):
        """__init__"""
        self.cachedir = "/var/tmp"

    @calllogger
    def GET(self, args):
        """
        get values from RAW Archive

        multiplexer for more functions
        """
        try:
            logging.debug("GET called, args=%s", args)
            method = args.split("/")[0]
            logging.info("method %s should be called", method)
            web.header('Access-Control-Allow-Origin', '*')
            web.header('Access-Control-Allow-Credentials', 'true')
            method_args = args.split("/")[1:] # all without method name
            if method == "getKeys": return(self.get_keys(method_args))
            elif method == "get_headers": return(self.get_headers(method_args))
            elif method == "get_index_keynames": return(self.get_index_keynames(method_args))
            elif method == "get_value_keynames": return(self.get_value_keynames(method_args))
            elif method == "get_ts_keyname": return(self.get_ts_keyname(method_args))
            elif method == "get_projects": return(self.get_projects(method_args))
            elif method == "get_tablenames": return(self.get_tablenames(method_args))
            elif method == "get_headers": return(self.get_headers(method_args))
            elif method == "get_chart_data_ungrouped": return(self.get_chart_data_ungrouped(method_args))
            elif method == "get_keys": return(self.get_keys(method_args))
            else: return("There is no method called %s" % method)
        except StandardError as exc:
            logging.exception(exc)
            return(traceback.format_exc())

    @calllogger
    def get_projects(self, args):
        """
        get all available projects
        """
        projects = []
        blacklist = ("lost+found", "templates")
        for name in os.listdir(basedir):
            logging.info(name)
            if os.path.isdir(os.path.join(basedir, name)) and name not in blacklist:
                projects.append(name)
        return(json.dumps(projects))

    @calllogger
    def get_tablenames(self, args):
        """
        get available tablenames
        """
        project = args[0]
        tablenames = {}
        basedir_raw = os.path.join(basedir, project, "raw")
        for name in os.listdir(basedir_raw):
            if os.path.isfile(os.path.join(basedir_raw, name)):
                tablename = name.split("_")[0]
                tablenames[tablename] = True
        return(json.dumps(tablenames.keys()))

    @calllogger
    def get_headers(self, args):
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return(json.dumps(datalogger.get_headers()))

    @calllogger
    def get_index_keynames(self, args):
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return(json.dumps(datalogger.get_index_keynames()))

    @calllogger
    def get_value_keynames(self, args):
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return(json.dumps(datalogger.get_value_keynames()))

    @calllogger
    def get_ts_keyname(self, args):
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return(json.dumps(datalogger.get_ts_keyname()))

    @calllogger
    def get_groups(self, args):
        """
        get available tablenames
        """
        project = args[0]
        groups = {}
        basedir_rrd = os.path.join(basedir, project, "rrd")
        for name in os.listdir(basedir_rrd):
            logging.debug("Found File %s", name)
            if os.path.isfile(os.path.join(basedir_rrd, name)):
                filename = name.replace(".rrd", "") # get rid of .rrd
                tablename = filename.split("_")[0]
                keys = filename.split("_")[1:]
                if tablename in groups:
                    groups[tablename].append(keys)
                else:
                    groups[tablename] = [keys]
        return(json.dumps(groups))

    @calllogger
    def get_data(self, args):
        """
        get values from RAW Archive

        mandatory parameters
        /project/tablename/start/stop
        """
        try:
            logging.debug("get_data called with %s", args)
            project, tablename, start, stop = args
            user_data = web.input() # get parameters
            datalogger = DataLogger(basedir, project, tablename)
            # limit output with to these fields
            fields = None
            if "fields" in user_data:
                fields = user_data.fields.split(",")
            # whitelist only results with this keys equal
            # keyids must be in form
            # key1=value1, key2=value2, key3=value3 ...
            keyids = None
            if "keyids" in user_data:
                keyids = {}
                items = user_data.keyids.split(",")
                for item in items:
                    key, value = item.split(":")
                    keyids[key] = value
                logging.debug("found following keyids: %s", keyids)
            # start and stop values, must be integer
            stop = int(stop) # convert to int
            start = int(start)
            start_ts = int(time.time())
            stop_ts = int(time.time())
            if stop == 0: # if stop=0 this means now(), and start is relative to stop
                stop_ts = int(time.time())
                start_ts = stop_ts - start
            else:
                start_ts = start
                stop_ts = stop
            assert start_ts < stop_ts # always increasing time
            # get data and convert it to list, generators are not
            # serializable
            data = list(datalogger.get_data(start_ts, stop_ts, keys=keyids, fields=fields)) # has to be list not generator for json
            return(json.dumps(data))
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

    @calllogger
    def get_timeseries(self, args):
        """
        get values from RAW Archive

        mandatory parameters
        /project/tablename/start/stop/keyids

        keyids=hostname:srvszp2orb.tilak.cc means
        this is only useful if keyids are unique
        """
        try:
            logging.debug("get_timeseries called with %s", args)
            project, tablename, start, stop = args
            user_data = web.input() # get parameters
            datalogger = DataLogger(basedir, project, tablename)
            # limit output with to these fields
            fields = None
            if "fields" in user_data:
                fields = user_data.fields.split(",")
            # whitelist only results with this keys equal
            # keyids must be in form
            # key1=value1, key2=value2, key3=value3 ...
            keyids = None
            if "keyids" in user_data:
                keyids = {}
                items = user_data.keyids.split(",")
                for item in items:
                    key, value = item.split(":")
                    keyids[key] = value
                logging.debug("found following keyids: %s", keyids)
            else:
                return("required parameter keyids not specified")
            # start and stop values, must be integer
            stop = int(stop) # convert to int
            start = int(start)
            start_ts = int(time.time())
            stop_ts = int(time.time())
            if stop == 0: # if stop=0 this means now(), and start is relative to stop
                stop_ts = int(time.time())
                start_ts = stop_ts - start
            else:
                start_ts = start
                stop_ts = stop
            assert start_ts < stop_ts # always increasing time
            # get data and convert it to list, generators are not
            # serializable for json
            data = list(datalogger.get_timeseries(start_ts, stop_ts, keys=keyids, fields=fields)) # has to be list not generator for json
            return(json.dumps(data))
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

    @calllogger
    def get_timeseries_day(self, args):
        """
        get values from RAW Archive

        URL Parameters /project/tablename/datestring
        query parameters
            ts_keyname=<str>
            index_keynames=<tuple>
            index_values=<tuple>
            value_keynames=<tuple>

        returns json
        [
            [ts value1, value2, value3, ...]
            [ts value1, value2, value3, ...]
            [ts value1, value2, value3, ...]
        ]
        """
        try:
            logging.info("get_timeseries_day called with %s", args)
            project, tablename, datestring = args
            user_data = web.input() # get parameters
            # limit output with to these fields
            key = None
            if "key" in user_data:
                logging.debug(user_data.key)
                key = tuple(json.loads(user_data.key))
                logging.debug("key = %s", key)
            else:
                return("key is mandatory")
            datalogger = DataLogger(basedir, project, tablename)
            logging.info("calling datalogger.read_day(%s)", datestring)
            tsa = datalogger.read_day(datestring)
            try:
                result = tuple(tsa[key].dump_list())
                return(json.dumps(result))
            except KeyError as exc:
                logging.exception(exc)
                logging.error("Key %s not found in dataset", str(key))
                return("Key %s not found in dataset" % str(key))
        except StandardError as exc:
            return(traceback.format_exc())
            logging.exception(exc)
            raise exc

    @calllogger
    def get_vicenter_timeseries(self, args):
        """
        get values from RAW Archive

        mandatory parameters
        /project/tablename/start/stop/keyids

        keyids=hostname:srvszp2orb.tilak.cc means
        this is only useful if keyids are unique
        """
        try:
            logging.debug("get_vicenter_timeseries called with %s", args)
            # the same for all vicenter data
            project = "vicenter"
            ts_keyname = "ts"
            tablename, datestring, servername, instance = args
            index_keynames = ("hostname", "instance")
            key = (str(servername), str(instance))
            if tablename == "virtualMachineMemoryStats":
                index_keynames = ("hostname", )
                key = (str(servername), )
            cachefilename = os.path.join(self.cachedir, "get_vicenter_timeseries-%s.cache" % "_".join(args))
            if os.path.isfile(cachefilename):
                logging.info("cache dataset found")
                return(open(cachefilename, "rb").read())
            else:
                # getting data from raw
                datalogger = DataLogger(basedir, project, tablename)
                logging.info("calling dh.read_day(datalogger, %s, %s, %s)", ts_keyname, index_keynames, datestring)
                tsa = dh.read_day(datalogger, ts_keyname, index_keynames, datestring)
                result = tuple(tsa[key].dump_list())
                logging.info("storing json data in cachefile %s", cachefilename)
                json.dump(result, open(cachefilename, "wb"))
                return(json.dumps(result))
        except StandardError as exc:
            logging.exception(exc)
            return(traceback.format_exc())

    @calllogger
    def get_keys(self, args):
        # the same for all vicenter data
        project, tablename, datestring = args
        logging.info("project : %s", project)
        logging.info("tablename : %s", tablename)
        logging.info("datestring : %s", datestring)
        datalogger = DataLogger(basedir, project, tablename)
        keys = datalogger.get_keys(datestring)
        return json.dumps(keys)

    @calllogger
    def get_chart_data_ungrouped(self, args):
        """
        get values from RAW Archive

        mandatory parameters
        /project/tablename/start/stop/keyids

        keyids=hostname:srvszp2orb.tilak.cc means
        this is only useful if keyids are unique

        return data like this:
        [
            {
                name: "name of this series" usually this is the counter name
                data : [[ts, value], ...]
            },
            ...
        ]
        """
        # the same for all vicenter data
        project, tablename, datestring, keys_str, value_keys_str, datatype_str, group_str = args
        keys = tuple(json.loads(keys_str))
        value_keys = ()
        if json.loads(value_keys_str) is not None:
            value_keys = tuple(json.loads(value_keys_str))
        datatype = json.loads(datatype_str)
        group_by = ()
        if json.loads(group_str) is not None:
            group_by = (json.loads(group_str),)
        logging.info("project : %s", project)
        logging.info("tablename : %s", tablename)
        logging.info("datestring : %s", datestring)
        logging.info("keys : %s", keys)
        logging.info("value_keys : %s", value_keys)
        logging.info("datatype : %s", datatype)
        logging.info("group_by : %s", group_by)
        tsa = None
        datalogger = DataLogger(basedir, project, tablename)
        if len(group_by) == 0:
            try:
                tsa = datalogger.read_cachefile_single(datestring, keys)
            except StandardError as exc:
                logging.exception(exc)
                tsa = datalogger.read_day(datestring)
        else:
            tsa = datalogger.read_day(datestring)
        if datatype != u"absolute":
            new_value_keys = []
            for value_key in value_keys:
                new_value_key = None
                if datatype == "derive":
                    new_value_key = "%s_d" % value_key
                    logging.info("deriving %s to %s", value_key, new_value_key)
                    tsa.add_derive_col(value_key, new_value_key)
                elif datatype == "per_s":
                    new_value_key = "%s_s" % value_key
                    logging.info("deriving %s to %s", value_key, new_value_key)
                    tsa.add_per_s_col(value_key, new_value_key)
                tsa.remove_col(value_key)
                new_value_keys.append(new_value_key)
            value_keys = new_value_keys
        logging.info(tsa.get_value_keys())
        # grouping stuff if necessary
        data = None
        if len(group_by) > 0:
            logging.info("generating new key for left possible keys in grouped tsa")
            key_dict = dict(zip(datalogger.get_index_keynames(), keys))
            new_key = tuple((key_dict[key] for key in group_by))
            logging.info("key after grouping would be %s", new_key)
            logging.info("grouping tsa by %s", group_by)
            new_tsa = tsa.get_group_by_tsa(group_by, group_func=lambda a: sum(a))
            tsa = new_tsa
            data = tsa[new_key].dump_dict()
        else:
            data = tsa[keys].dump_dict()
        result = []
        logging.info("data keys : %s", data[data.keys()[0]].keys())
        for value_key in value_keys:
            # ist important to sort by timestamp, to not confuse
            # highcharts
            result.append({
                "name" : value_key,
                "data" : tuple(((ts * 1000, row_dict[value_key]) for ts, row_dict in sorted(data.items())))
                }
            )
        return(json.dumps(result))


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()
