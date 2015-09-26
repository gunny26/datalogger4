#!/usr/bin/python

import web
import os
import logging
logging.basicConfig(level=logging.INFO)
import logging.handlers
#import hashlib
import json
#import datetime
import time
#import traceback
#import cPickle
#import gzip
#import base64
#from string import Template
# own modules
from tilak_datalogger import DataLogger as DataLogger
#from tilak_datalogger import DataLoggerHelper as dh
#from tilak_datalogger import TimeseriesArray as TimeseriesArray

urls = (
    "/RawData/(.*)", "DataloggerWeb",
    "/DataloggerWeb/(.*)", "DataloggerWeb",
    )

# add wsgi functionality
#project = "vicenter"
basedir = "/var/rrd"
#basedir_templates = os.path.join(basedir, "templates")
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

class DataloggerWeb(object):
    """retrieve Data from RRD Archive"""

    def __init__(self):
        """__init__"""

    @calllogger
    def GET(self, args):
        """
        GET Multiplexer function, according to first argument in URL
        call this function, and resturn result to client

        parameters:
        /<str>function_name/...

        return:
        return function_name(what is left of arguments)

        """
        method = args.split("/")[0]
        logging.info("method %s should be called", method)
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        method_args = args.split("/")[1:] # all without method name
        if method == "get_headers":
            return self.get_headers(method_args)
        elif method == "get_index_keynames":
            return self.get_index_keynames(method_args)
        elif method == "get_value_keynames":
            return self.get_value_keynames(method_args)
        elif method == "get_ts_keyname":
            return self.get_ts_keyname(method_args)
        elif method == "get_projects":
            return self.get_projects(method_args)
        elif method == "get_tablenames":
            return self.get_tablenames(method_args)
        elif method == "get_headers":
            return self.get_headers(method_args)
        elif method == "get_chart_data_ungrouped":
            return self.get_chart_data_ungrouped(method_args)
        elif method == "get_keys":
            return self.get_keys(method_args)
        else:
            return "There is no method called %s" % method

    @calllogger
    def get_projects(self, args):
        """
        get all available projects, use directory listing for that,
        but blacklist some non project directories

        parameters:
        None

        returns:
        <json><list> of existing project names
        """
        projects = []
        blacklist = ("lost+found", "templates", "global_cachedir")
        for name in os.listdir(basedir):
            if os.path.isdir(os.path.join(basedir, name)) and name not in blacklist:
                projects.append(name)
        return json.dumps(projects)

    @calllogger
    def get_tablenames(self, args):
        """
        get available tablenames, for one particular project
        uses directory listing in raw subdirectory for this purpose

        parameters:
        <str>projectname

        returns:
        <json><list> of possible tablenames
        """
        assert len(args) == 1
        project = args[0]
        tablenames = {}
        basedir_raw = os.path.join(basedir, project, "raw")
        for name in os.listdir(basedir_raw):
            if os.path.isfile(os.path.join(basedir_raw, name)):
                tablename = name.split("_")[0]
                tablenames[tablename] = True
        return json.dumps(tablenames.keys())

    @calllogger
    def get_headers(self, args):
        """
        get name of headers (all columns so ts_keyname + index_keynames + value_keynames)

        parameters:
        /<str>project/<str>tablename

        returns:
        <json><list> of header names
        """
        assert len(args) == 2
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return json.dumps(datalogger.get_headers())

    @calllogger
    def get_index_keynames(self, args):
        """
        get name of index columns for project/tablename

        parameters:
        /<str>project/<str>tablename

        returns:
        <json><list> columns names of index columns defined
        """
        assert len(args) == 2
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return json.dumps(datalogger.get_index_keynames())

    @calllogger
    def get_value_keynames(self, args):
        """
        get name of value columns for project/tablename
        all value_keynames have to be strictly numeric

        parameters:
        /<str>project/<str>tablename

        returns:
        <json><list> column names of value columns defined
        """
        assert len(args) == 2
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return json.dumps(datalogger.get_value_keynames())

    @calllogger
    def get_ts_keyname(self, args):
        """
        get name of timestamp column

        parameters:
        /<str>project/<str>tablename

        returns:
        <json><list> column names of value columns defined
        """
        assert len(args) == 2
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        return json.dumps(datalogger.get_ts_keyname())

    @calllogger
    def get_keys(self, args):
        """
        get name of all index keys found in one specific TimeseriesArray

        parameters:
        /<str>project/<str>tablename/<str>datestring

        returns:
        <json><list> of all index combinations
        """
        # the same for all vicenter data
        assert len(args) == 3
        project, tablename, datestring = args
        datalogger = DataLogger(basedir, project, tablename)
        keys = datalogger.get_keys(datestring)
        return json.dumps(keys)

    @calllogger
    def get_chart_data_ungrouped(self, args):
        """
        get values from RAW Archive

        parameters:
        /<str>project/<str>tablename/<str>datestring/<str>key/<str>value_keys/<str>datetype/<str>group_str

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
        assert len(args) == 7
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
        # if there is nothing to group, try to read from cached, or read
        # day from cPickle dump
        if len(group_by) == 0:
            try:
                tsa = datalogger.read_cachefile_single(datestring, keys)
            except StandardError as exc:
                logging.exception(exc)
                tsa = datalogger.read_day(datestring)
        else:
            # otherwise read from raw file
            tsa = datalogger.read_day(datestring)
        # is there something to calculate, lets do it
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
        #logging.info(tsa.get_value_keys())
        # grouping stuff if necessary
        data = None # holds finally calculated data
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
        result = [] # holds return data
        logging.info("data keys : %s", data[data.keys()[0]].keys())
        for value_key in value_keys:
            # ist important to sort by timestamp, to not confuse
            # highcharts
            result.append(
                {
                    "name" : value_key,
                    "data" : tuple(((ts * 1000, row_dict[value_key]) for ts, row_dict in sorted(data.items())))
                }
            )
        return json.dumps(result)


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()
