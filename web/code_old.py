#!/usr/bin/python

import web
import os
import logging
logging.basicConfig(level=logging.DEBUG)
import logging.handlers
import json
import datetime
import time
import rrdtool
import traceback
from string import Template
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import DataLoggerHelper as dh

urls = ("/RrdData/(.*)", "RrdData",
        "/RawData/(.*)", "RawData",
        "/Gfx/(.*)", "Gfx",
        "/testclass/(.*)", "TestClass",
        "/testjson/(.*)", "TestJson",
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


class DataLogger_old(object):

    def __init__(self, basedir, project, tablename, delimiter="\t"):
        self.basedir = os.path.join(basedir, project)
        self.tablename = tablename
        self.delimiter = delimiter
        # defining variables
        self.headers = None
        self.gfx_basedir = os.path.join(self.basedir, "gfx")
        self.raw_basedir = os.path.join(self.basedir, "raw")
        self.rrd_basedir = os.path.join(self.basedir, "rrd")
        # get header information
        self.__read_headers()

    def parse_line(self, row):
        """specialized method to parse line read from CSV"""
        data = {}
        values = row.split(self.delimiter)
        index = 0
        for header in self.headers:
            data[header] = values[index]
            index +=1
        # convert timestamp in numeric type
        data["ts"] = int(data["ts"].split(".")[0])
        return(data)

    def __read_headers(self):
        """get header information from last file"""
        filename = os.path.join(self.raw_basedir, "%s_%s.csv" % (self.tablename, datetime.date.today()))
        headers = open(filename, "rb").readline()
        self.headers = headers.strip().split(self.delimiter)
        logging.debug("reading headers from filename %s : %s", filename, self.headers)

    def get_headers(self):
        return self.headers

    def get_filenames(self, start_ts, stop_ts):
        """
        try to find valuable files to read from
        """
        # get timestamps into datetime.date format
        assert start_ts < stop_ts
        startdate = datetime.datetime.fromtimestamp(start_ts).date()
        stopdate = datetime.datetime.fromtimestamp(stop_ts).date()
        running_date = startdate # goes from start to stop interval 1 day
        while running_date <= stopdate:
            # build possible filename
            filename = os.path.join(self.raw_basedir, "%s_%s.csv" % (self.tablename, running_date.isoformat()))
            # check if file exists
            if os.path.isfile(filename):
                logging.debug("File %s exists", filename)
                yield filename
            else:
                logging.debug("File %s not available, skipping", filename)
            # count up running_date
            running_date = running_date + datetime.timedelta(days=1) # add up 1 day

    def get_data(self, start_ts, stop_ts, keys=None, fields=None):
        """
        reads data from raw datafiles, filters by keys if given, and returns only keys in fields if given

        attention: raw data my have duplicate timestamp values for each

        The resulting data can have duplicate keys
        """
        # for every possible file in given timerange
        # reads data from raw datafiles
        logging.debug("called get_data(start_ts=%s, stop_ts=%s, keys=%s, fields=%s)", start_ts, stop_ts, keys, fields)
        for data in self.__read_row(start_ts, stop_ts):
            if start_ts <= data["ts"] <= stop_ts:
                if keys is not None:
                    if not self.keyfilter_matched(data, keys):
                        continue
                if fields is not None:
                    yield tuple((data[key] for key in fields))
                else:
                    yield data.values()

    def __read_row(self, start_ts, stop_ts):
        """
        generator to return one parsed line from raw files where
        ts timestamp matches between start_ts and stop_ts
        """
        logging.debug("called __read_row(start_ts=%s, stop_ts=%s)", start_ts, stop_ts)
        for filename in self.get_filenames(start_ts, stop_ts):
            assert start_ts < stop_ts
            lineno = 0
            for row in open(filename, "rb"):
                lineno += 1
                if row.strip().startswith(self.headers[0]): # skip header line
                    continue
                try:
                    data = self.parse_line(row.strip())
                    if data["ts"] > stop_ts: # stop if data is above stop_ts
                        return
                    elif data["ts"] < start_ts:
                        continue
                    yield self.parse_line(row.strip())
                except IndexError as exc:
                    logging.exception(exc)
                    logging.error("Format Error in File %s, line %s", filename, lineno)
                    logging.error("Data: %s", row.strip())

    def get_timeseries(self, start_ts, stop_ts, keys, fields=None):
        """
        reads data from raw datafiles, filters by keys if given, and returns only keys in fields if given

        attention: raw data my have duplicate timestamp values for each

        TODO: this does not work, in special vicenter raw date there may be duplicate rows!!!!
        """
        # for every possible file in given timerange
        # reads data from raw datafiles
        logging.debug("called get_timeseries(start_ts=%s, stop_ts=%s, keys=%s, fields=%s)", start_ts, stop_ts, keys, fields)
        datalist = []
        last_ts = 0 # latest read timestamp
        for data in self.__read_row(start_ts, stop_ts):
            # raw data must be conituous in ts values
            if not self.keyfilter_matched(data, keys):
                continue # skip this data of keys doesnt match
            if data["ts"] > last_ts:
                last_ts = data["ts"]
            else:
                # there is some timewarp, older data after
                # newer, maybe not sorted, or duplicate. first
                # row wins
                continue # skip this row
            if start_ts <= data["ts"] <= stop_ts:
                if fields is not None:
                    yield tuple((data[key] for key in fields))
                else:
                    yield data.values()

    def keyfilter_matched(self, data, keys):
        for key in keys:
            if keys[key] != data[key]:
                return False # if any key differs, next line
        return True

    def get_data_for_keys(self, start_ts, stop_ts, keys, fields=None):
        """
        get data from raw file from start_ts to stop_ts

        return only line who match keys criteria
        keys = {
            fieldname1 : value1,
            fieldname2 : value2,
            }
        with line data
        return only keys in fields list, if not None
        """
        # for every possible file in given timerange
        for filename in self.get_filenames(start_ts, stop_ts):
            lineno = 0
            for row in open(filename, "rb"):
                lineno += 1
                if row.strip().startswith(self.headers[0]): # skip header line
                    continue
                try:
                    data = self.parse_line(row.strip())
                    if start_ts <= data["ts"] <= stop_ts:
                        if not self.keyfilter_matched(data, keys):
                            continue
                        if fields is not None:
                            yield dict(((key, data[key]) for key in fields))
                        else:
                            yield data
                except IndexError as exc:
                    logging.exception(exc)
                    logging.error("Format Error in File %s, line %s", filename, lineno)
                    logging.error("Data: %s", row.strip())

    def get_keys(self, start_ts, stop_ts, fields):
        """
        get data from raw file from start_ts to stop_ts

        return only line who match keys criteria
        keys = {
            fieldname1 : value1,
            fieldname2 : value2,
            }
        with line data
        return only keys in fields list, if not None
        """
        # for every possible file in given timerange
        keys_dict = {}
        for filename in self.get_filenames(start_ts, stop_ts):
            lineno = 0
            for row in open(filename, "rb"):
                lineno += 1
                if row.strip().startswith(self.headers[0]): # skip header line
                    continue
                try:
                    data = self.parse_line(row.strip())
                    if start_ts <= data["ts"] <= stop_ts:
                        keys_dict[tuple((data[key] for key in fields))] = True
                except IndexError as exc:
                    logging.exception(exc)
                    logging.error("Format Error in File %s, line %s", filename, lineno)
                    logging.error("Data: %s", row.strip())
        return(keys_dict.keys())

    def rrdfetch(self, rrdfilename, start_ts, stop_ts=None, step=300):
        """
        get data from rrd archive

        rrdfilen <str> absolute name of file to read from, must be rrd format
        start_ts <numeric> timstamp of start time
        stop_ts <numeric> timestamp of stop time
        step <numeric> resolution  in seconds
        return tuple of three variables
        (
            timeinfo <tuple> of three values (start_ts, stop_ts, step)
            headers <tuple> of column names,
            data <tuple> of <tuple> of values
        )
        """
        max_ts = rrdtool.last(rrdfilename)
        if stop_ts is None:
            stop_ts = max_ts
        else:
            assert max_ts <= stop_ts
        if start_ts < max_ts:
            start_ts = max_ts - step
        timeinfo, headers, data = rrdtool.fetch(rrdfilename,
            "AVERAGE",
            "-r", "%d" % step,
            "-s", "%d" % start_ts,
            "-e", "%d" % stop_ts
            )
        return(timeinfo, headers, data)

class TestClass(object):
    """To test Requests"""

    def __init__(self):
        self.logger = logging.getLogger("")

    def get_debug_headers(self):
        sb = ""
        for key, value in web.ctx.environ.items():
            sb += "%s: %s\n" % (key, value)
        return(sb)

    def GET(self, parameters):
        self.logger.info("GET called")
        if web.ctx.environ["REQUEST_METHOD"] == "HEAD":
            return(self.head(parameters))
        return("Method GET called, URL-Parameter=%s, data=%s\n%s" % (parameters, web.data(), self.get_debug_headers()))

    def PUT(self, parameters):
        self.logger.info("PUT called")
        return("Method PUT called, URL-Parameter=%s, data=%s" % (parameters, web.data()))

    def POST(self, parameters):
        self.logger.info("POST called")
        return("Method POST called, URL-Parameter=%s, data=%s" % (parameters, web.data()))

    def DELETE(self, parameters):
        self.logger.info("DELETE called")
        return("Method DELETE called, URL-Parameter=%s, data=%s" % (parameters, web.data()))

    def OPTION(self, parameters):
        self.logger.info("OPTION called")
        return("Method OPTION called, URL-Parameter=%s, data=%s" % (parameters, web.data()))

    def HEAD(self, parameters):
        # HEAD does not work, every request through apache results in GET
        # no idea why?
        self.logger.info("HEAD called")
        # HEAD cannot return any DATA, so only status indicates if something is true or false
        web.notfound()

    def OTHER(self, parameters):
        self.logger.info("OTHER called")
        return("Method OTHER called, URL-Parameter=%s, data=%s" % (parameters, web.data()))


class TestJson(object):
    """To test JSON Requests"""

    def __init__(self):
        self.testdata = {"vorname" : "Arthur", "nachname" : "Messner"}
        self.logger = logging.getLogger("")

    def get_debug_headers(self):
        sb = ""
        for key, value in web.ctx.environ.items():
            sb += "%s: %s\n" % (key, value)
        return(sb)

    def GET(self, parameters):
        self.logger.info("GET called")
        # add these two headers to allow jquery request from other sites
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        data = json.loads(open("/var/www/static/test.json", "rb").readline())
        #for i in range(1000):
        #    data[i] = i * 2
        return(json.dumps(data))


class RrdData(object):
    """retrieve Data from RRD Archive"""

    def __init__(self):
        """__init__"""
        self.logger = logging.getLogger("")

    def rrdfetch(self, rrdfilename, start_ts, stop_ts=None, step=300):
        """
        get data from rrd archive

        rrdfilen <str> absolute name of file to read from, must be rrd format
        start_ts <numeric> timstamp of start time
        stop_ts <numeric> timestamp of stop time
        step <numeric> resolution  in seconds
        return tuple of three variables
        (
            timeinfo <tuple> of three values (start_ts, stop_ts, step)
            headers <tuple> of column names,
            data <tuple> of <tuple> of values
        )
        """
        max_ts = rrdtool.last(rrdfilename)
        if stop_ts is None:
            stop_ts = max_ts
        else:
            assert max_ts <= stop_ts
        if start_ts < max_ts:
            start_ts = max_ts - step
        timeinfo, headers, data = rrdtool.fetch(rrdfilename,
            "AVERAGE",
            "-r", "%d" % step,
            "-s", "%d" % start_ts,
            "-e", "%d" % stop_ts
            )
        return(timeinfo, headers, data)

    def GET(self, args):
        """
        get values from RRD Archive
        """
        try:
            self.logger.debug("GET called, args=%s", args)
            project, tablename, keyid, start, stop = args.split("/")
            stop = int(stop)
            start = int(start)
            stop_ts = time.time()
            if stop != 0:
                stop_ts = stop
            start_ts = stop_ts - start
            basedir_rrd = os.path.join(basedir, project, "rrd")
            rrd_filename = os.path.join(basedir_rrd, "%s_%s.rrd" % (tablename, keyid))
            timeinfo, headers, data = self.rrdfetch(str(rrd_filename), start_ts, stop_ts, 20)
            web.header('Access-Control-Allow-Origin', '*')
            web.header('Access-Control-Allow-Credentials', 'true')
            ret_data = {
                "start" : timeinfo[0],
                "stop" : timeinfo[1],
                "step" : timeinfo[2],
                "headers" : headers,
                "data" : data
            }
            return(json.dumps(ret_data))
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

class RawData(object):
    """retrieve Data from RRD Archive"""

    def __init__(self):
        """__init__"""
        self.cachedir = "/var/tmp"

    def rrdfetch(self, rrdfilename, start_ts, stop_ts=None, step=300):
        """
        get data from rrd archive

        rrdfilen <str> absolute name of file to read from, must be rrd format
        start_ts <numeric> timstamp of start time
        stop_ts <numeric> timestamp of stop time
        step <numeric> resolution  in seconds
        return tuple of three variables
        (
            timeinfo <tuple> of three values (start_ts, stop_ts, step)
            headers <tuple> of column names,
            data <tuple> of <tuple> of values
        )
        """
        max_ts = rrdtool.last(rrdfilename)
        if stop_ts is None:
            stop_ts = max_ts
        else:
            assert max_ts <= stop_ts
        if start_ts < max_ts:
            start_ts = max_ts - step
        timeinfo, headers, data = rrdtool.fetch(rrdfilename,
            "AVERAGE",
            "-r", "%d" % step,
            "-s", "%d" % start_ts,
            "-e", "%d" % stop_ts
            )
        return(timeinfo, headers, data)

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
            if method == "getKeys": return(self.get_keys(args.split("/")[1:]))
            elif method == "getData": return(self.get_data(args.split("/")[1:]))
            elif method == "getTimeseries": return(self.get_timeseries(args.split("/")[1:]))
            elif method == "getTimeseriesDay": return(self.get_timeseries_day(args.split("/")[1:]))
            elif method == "getProjects": return(self.get_projects(args.split("/")[1:]))
            elif method == "getTablenames": return(self.get_tablenames(args.split("/")[1:]))
            elif method == "getHeaders": return(self.get_headers(args.split("/")[1:]))
            elif method == "getGroups": return(self.get_groups(args.split("/")[1:]))
            elif method == "getVicenterTimeseries": return(self.get_vicenter_timeseries(args.split("/")[1:]))
            else: return(None)
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

    def get_projects(self, args):
        """
        get all available projects
        """
        try:
            projects = []
            blacklist = ("lost+found", "templates")
            for name in os.listdir(basedir):
                logging.info(name)
                if os.path.isdir(os.path.join(basedir, name)) and name not in blacklist:
                    projects.append(name)
            return(json.dumps(projects))
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

    def get_tablenames(self, args):
        """
        get available tablenames
        """
        try:
            project = args[0]
            tablenames = {}
            basedir_raw = os.path.join(basedir, project, "raw")
            for name in os.listdir(basedir_raw):
                if os.path.isfile(os.path.join(basedir_raw, name)):
                    tablename = name.split("_")[0]
                    tablenames[tablename] = True
            return(json.dumps(tablenames.keys()))
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

    def get_groups(self, args):
        """
        get available tablenames
        """
        logging.debug("get_groups(%s) called", args)
        try:
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
        except StandardError as exc:
            logging.exception(exc)
            return(traceback.format_exc())
            raise exc

    def get_headers(self, args):
        """
        get header information of raw data
        """
        try:
            project, tablename = args
            datalogger = DataLogger(basedir, project, tablename)
            headers = datalogger.get_headers()
            return(json.dumps(headers))
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

    def get_keys(self, args):
        """
        get values from RAW Archive
        """
        try:
            project, tablename, start, stop = args
            user_data = web.input()
            logging.info(user_data)
            keyids = []
            if "keyids" in user_data:
                keyids = user_data.keyids.split(",")
            datalogger = DataLogger(basedir, project, tablename)
            stop = int(stop)
            start = int(start)
            stop_ts = int(time.time())
            if stop != 0:
                stop_ts = stop
            start_ts = stop_ts - start
            keyids = datalogger.get_keys(start_ts, stop_ts, keyids)
            logging.info(keyids)
            return(json.dumps(keyids))
        except StandardError as exc:
            return(traceback.format_exc())
            raise exc

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

    def get_timeseries_day(self, args):
        """
        get values from RAW Archive

        mandatory parameters
        /project/tablename/start/stop/keyids

        keyids=hostname:srvszp2orb.tilak.cc means
        this is only useful if keyids are unique
        """
        try:
            logging.debug("get_timeseries_day called with %s", args)
            project, tablename, datestring = args
            user_data = web.input() # get parameters
            # limit output with to these fields
            ts_keyname = "ts" # default value
            if "ts_keyname" in user_data:
                ts_keyname = str(user_data.ts_keyname)
                logging.debug("ts_keyname = %s", ts_keyname)
            value_keynames = None
            if "value_keynames" in user_data:
                value_keynames = (str(item) for item in user_data.value_keynames.split(","))
                logging.debug("value_keynames = %s", tuple(value_keynames))
            index_keynames = None
            if "index_keynames" in user_data:
                index_keynames = (str(item) for item in user_data.index_keynames.split(","))
                logging.debug("index_keynames = %s", tuple(index_keynames))
            index_values = None
            if "index_values" in user_data:
                index_values = (str(item) for item in user_data.index_values.split(","))
                logging.debug("index_values = %s", tuple(index_values))
            datalogger = DataLogger(basedir, project, tablename)
            logging.info("calling dh.read_day(datalogger, %s, %s, %s)", (ts_keyname, index_keynames, datestring))
            tsa = dh.read_day(datalogger, ts_keyname, index_keynames, datestring)
            result = tuple(tsa[index_values].dump_list(value_keynames))
            return(json.dumps(result))
        except StandardError as exc:
            return(traceback.format_exc())
            logging.exception(exc)
            raise exc

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


class Gfx(object):
    """retrieve Data from RRD Archive"""

    def __init__(self):
        """__init__"""
        self.logger = logging.getLogger("")

    def rrdfetch(self, rrdfilename, start_ts, stop_ts=None, step=300):
        """
        get data from rrd archive

        rrdfilen <str> absolute name of file to read from, must be rrd format
        start_ts <numeric> timstamp of start time
        stop_ts <numeric> timestamp of stop time
        step <numeric> resolution  in seconds
        return tuple of three variables
        (
            timeinfo <tuple> of three values (start_ts, stop_ts, step)
            headers <tuple> of column names,
            data <tuple> of <tuple> of values
        )
        """
        max_ts = rrdtool.last(rrdfilename)
        if stop_ts is None:
            stop_ts = max_ts
        else:
            assert max_ts <= stop_ts
        assert start_ts < max_ts
        timeinfo, headers, data = rrdtool.fetch(rrdfilename,
            "AVERAGE",
            r"-r", "%d" % step,
            "-s", "%d" % start_ts,
            "-e", "%d" % stop_ts
            )
        return(timeinfo, headers, data)

    def create_graphic(self, *args, **kwds):
        """
        create rrd graphics from rrd archive

        project
        tablename
        start
        stop
        keyid

        temporary filename is chosen automatically
        """
        self.logger.info(kwds)
        basedir_rrd = os.path.join(basedir, kwds["project"], "rrd") # path to RRD files
        kwds["rrd_filename"] = os.path.join(basedir_rrd, "%s_%s.rrd" % (kwds["tablename"], kwds["keyid"])) # RRD filename
        template_filename =  os.path.join(basedir, "templates", "%s_%s.def" %(kwds["project"], kwds["template"])) # template filename
        tempfile = os.path.join("/tmp", "%s_%s.png" %(kwds["project"], kwds["tablename"])) # temporary file
        template = Template(open(template_filename, "rb").read())
        s_template = template.substitute(kwds)
        # every argument to rrdtool has to be str not unicode
        rrdtool.graph(str(tempfile), list((str(item) for item in s_template.split("\n")[:-1])))
        return(tempfile)

    def GET(self, args):
        """
        create rrdgraph on the fly, and resturn image/jpeg created
        """
        try:
            self.logger.debug("GET called, args=%s", args)
            project, tablename, template, keyid, start, stop = args.split("/")
            stop = int(stop)
            start = int(start)
            stop_ts = time.time()
            if stop != 0:
                stop_ts = stop
            start_ts = stop_ts - start
            gfx_file = self.create_graphic(project=project, tablename=tablename, template=template, keyid=keyid, start=int(start_ts), stop=int(stop_ts))
            web.header('Content-Type', 'image/jpeg')
            return(open(gfx_file, "rb").read())
        except StandardError as exc:
            logging.exception(exc)
            return(traceback.format_exc())
            raise exc


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()
