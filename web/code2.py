#!/usr/bin/python

import web
import os
#import gc
#gc.set_debug(gc.DEBUG_STATS)
import logging
logging.basicConfig(level=logging.INFO)
#import logging.handlers
import json
import time
import base64
import gzip
# own modules
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesStats as TimeseriesStats

urls = (
    "/(.*)", "DataLoggerWeb",
    )

basedir = "/var/rrd"
application = web.application(urls, globals()).wsgifunc()
#handler = logging.handlers.RotatingFileHandler(
#    os.path.join(basedir, "/var/log/apache2/datalogger.log"),
#    maxBytes=10 * 1024 * 1024,
#    backupCount=5)
#logging.getLogger("").addHandler(handler)
#logging.getLogger("").setLevel(level=logging.DEBUG)

def calllogger(func):
    """
    decorator to log and measure call durations
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
            return "call to %s caused StandardError" % call_str
    # set inner function __name__ and __doc__ to original ones
    inner.__name__ = func.__name__
    inner.__doc__ = func.__doc__
    return inner

MEMCACHE = {}
MAXAGE = 300
def memcache(func):
    """
    decorator to cache return values according to used function parameters
    """
    def inner(*args, **kwds):
        starttime = time.time()
        thiskey = unicode((func.__name__, args, kwds))
        logging.info("number of keys in cache %d", len(MEMCACHE.keys()))
        logging.info("key to look for %s", thiskey)
        # get rid of old cache entries
        for key in MEMCACHE.keys():
            if (MEMCACHE[key]["ts"] + MAXAGE) < starttime:
                logging.info("deleting aged cache entry for key %s", key)
                del MEMCACHE[key]
        # is there an entry for this key
        if thiskey in MEMCACHE:
            if (MEMCACHE[thiskey]["ts"] + MAXAGE) > starttime:
                logging.info("returning from cache for key %s", thiskey)
                return MEMCACHE[thiskey]["data"]
        #logging.info("createing new cache entry for %s", key)
        try:
            ret_val = func(*args, **kwds)
            #logging.info("Storing returned data in cache for %s s", MAXAGE)
            MEMCACHE[thiskey] = {
                "ts" : starttime,
                "data" : ret_val,
            }
            return ret_val
        except StandardError as exc:
            logging.exception(exc)
    # set inner function __name__ and __doc__ to original ones
    inner.__name__ = func.__name__
    inner.__doc__ = func.__doc__
    return inner



class DataLoggerWeb(object):
    """retrieve Data from RRD Archive"""

    def __init__(self):
        """__init__"""

    def GET(self, args):
        """
        GET Multiplexer function, according to first argument in URL
        call this function, and resturn result to client

        parameters:
        /<str>function_name/...

        return:
        return function_name(what is left of arguments)

        / -> return projects
        /<projectname>/ -> return tablenames
        /<projectname>/<tablename>/ -> return table specification (index_keynames, headers, value_keynames, ts_keyname)
        /<projectname>/<tablename>/tsa/<datestring> -> get tsa of this datestring
        /<projectname>/<tablename>/ts/<datestring> -> get ts of this datestring
        /<projectname>/<tablename>/quantile/<datestring> -> get quantile of this datestring
        /<projectname>/<tablename>/tsastat/<datestring> -> get tsastat of this datestring
        """
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        web.header("Content-Type", "application/json")
        logging.info("received args %s", args)
        if args == u"":
            return self.get_projects()
        # strip trailing slash
        if args[-1] == u"/":
            args = args[:-1]
        parameters = args.split("/")
        if len(parameters) > 0:
            logging.info("received parameters %s", parameters)
            project = parameters[0]
            if len(parameters) == 1:
                return self.get_tablenames(project)
            elif len(parameters) == 2:
                tablename = parameters[1]
                return self.get_meta([project, tablename])
            elif len(parameters) == 3:
                tablename = parameters[1]
                datestring = parameters[2]
                return self.get_caches([project, tablename, datestring])
            elif len(parameters) >= 4:
                tablename = parameters[1]
                datestring = parameters[2]
                function = parameters[3]
                args = []
                if len(parameters) > 4:
                    args = parameters[4:]
                if function == "tsa":
                    return self.get_tsa(project, tablename, datestring, args)
                elif function == "ts":
                    return self.get_ts(project, tablename, datestring, args)
                elif function == "tsastats":
                    return self.get_tsastats(project, tablename, datestring, args)
                elif function == "quantile":
                    return self.get_quantile(project, tablename, datestring, args)
        else:
            logging.debug("unknown method called %s", method)
            return "unknown call %s" % parameters

    def POST(self, args):
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
        #web.header('Access-Control-Allow-Origin', '*')
        #web.header('Access-Control-Allow-Credentials', 'true')
        method_args = args.split("/")[1:] # all without method name
        if method == "upload_raw_file":
            return self.upload_raw_file(method_args)
        else:
            return "There is no method called %s" % method

    def doc(self, args):
        """
        get docstrings from methods available

        ex: DataLogger/doc/get_projects/something/or/nothing

        only the first argument after doc is evaluated,
        the remaining is ignored
        """
        # use only the fist argument to find function
        web.header("Content-Type", "text/html")
        outbuffer = ["<html><body>"]
        try:
            func = eval("self.%s" % args[0])
            doc = func.__doc__
            name = func.__name__
            outbuffer.append("<h1 class=datalogger-function-name>def %s(*args, **kwds)</h1>" % name)
            outbuffer.append("<div class=datalogger-function-doc>")
            if doc is not None:
                outbuffer.append(doc.replace("\n", "<br>"))
            outbuffer.append("</div>")
        except AttributeError as exc:
            logging.info(exc)
            outbuffer.append(str(exc))
        outbuffer.append("</body></html>")
        return "\n".join(outbuffer)

    @staticmethod
    @memcache
    def get_projects():
        """
        get available projects for this Datalogger Server

        ex: Datalogger/get_projects/...
        there is no further argument needed

        returns:
        json(existing project names)
        """
        ret_data = {
            "projects" : DataLogger.get_projects(basedir),
            "stat_func_names" : TimeseriesStats.stat_funcs.keys(),
            "last_businessday_datestring" : DataLogger.get_last_business_day_datestring()
        }
        return json.dumps(ret_data)

    @staticmethod
    @memcache
    def get_tablenames(project):
        """
        get available tablenames, for one particular project
        uses directory listing in raw subdirectory for this purpose

        ex: Datalogger/get_tablenames/{projectname}
        <projectname> has to be something from Datalogger/get_projects

        returns:
        json(list of possible tablenames for given project)
        """
        return json.dumps(DataLogger.get_tablenames(basedir, project))

    @staticmethod
    @memcache
    def get_meta(args):
        """
        get available tablenames, for one particular project
        uses directory listing in raw subdirectory for this purpose

        ex: Datalogger/get_tablenames/{projectname}
        <projectname> has to be something from Datalogger/get_projects

        returns:
        json(list of possible tablenames for given project)
        """
        project, tablename = args
        datalogger = DataLogger(basedir, project, tablename)
        ret_data = datalogger.meta
        # correcting value_keynames, to give only the keynames not the
        # types
        ret_data["value_keynames"] = ret_data["value_keynames"].keys()
        return json.dumps(datalogger.meta)

    @staticmethod
    def get_caches(args):
        """
        return dictionary of caches available for this project/tablename/datestring combination

        ex: Datalogger/get_caches/{project}/{tablename}/{datestring}

        {
            "tsastat" : {
                "keys" : dictionary of available keys,
                "pattern" : filename pattern,
            },
            "tsstat" : {
                "keys" : dictionary of available keys,
                "pattern" : filename pattern,
            },
            "tsa":
                "keys" : dictionary of available keys,
                "pattern" : filename pattern,
            },
            "ts" : {
                "keys" : dictionary of available keys,
                "pattern" : filename pattern,
            },
            "raw" : None or filename of raw data,
        }

        if return_date["raw"] == null it means, there is no raw data available
        else if something (tsa,ts,tsastat,tsstat) is missing you can call get_tsastat to generate all caches

        returns:
        json(dictionary of caches and available data)
        """
        project, tablename, datestring = args[:3]
        datalogger = DataLogger(basedir, project, tablename)
        caches = {}
        try:
            caches = datalogger.get_caches(datestring)
        except StandardError as exc:
            logging.exception(exc)
            logging.error(caches)
        return json.dumps(caches)

    def get_tsa(self, args):
        """
        return exported TimeseriesArray json formatted
        """
        project, tablename, datestring = args
        datalogger = DataLogger(basedir, project, tablename)
        tsa = datalogger[datestring]
        web.header('Content-type', 'text/html')
        # you must not set this option, according to
        # http://stackoverflow.com/questions/11866333/ioerror-when-trying-to-serve-file
        # web.header('Transfer-Encoding','chunked')
        yield "[" + json.dumps(tsa.export().next())
        for chunk in tsa.export():
            #logging.info("yielding %s", chunk)
            yield "," + json.dumps(chunk)
        yield "]"

    def get_tsa_adv(self, args):
        """
        return exported TimeseriesArray json formatted
        """
        group_funcs = {
            "avg" : lambda a, b: (a+b)/2,
            "min" : min,
            "max" : max,
            "sum" : lambda a, b: a+b,
        }
        logging.info(args)
        project, tablename, datestring, groupkeys_enc, group_func_name, index_pattern_enc = args
        groupkeys_dec = eval(base64.b64decode(groupkeys_enc)) # should be tuple
        logging.info("groupkeys_dec: %s", groupkeys_dec)
        index_pattern = base64.b64decode(index_pattern_enc)
        if index_pattern == "None":
            index_pattern = None
        logging.info("index_pattern: %s", index_pattern)
        assert group_func_name in group_funcs.keys()
        datalogger = DataLogger(basedir, project, tablename)
        tsa = None
        # gete data
        if groupkeys_dec is not None:
            logging.info("groupkeys is %s", groupkeys_dec)
            groupkeys = tuple([unicode(key_value) for key_value in groupkeys_dec])
            tsa1 = datalogger.load_tsa(datestring, index_pattern=index_pattern)
            tsa = datalogger.group_by(datestring, tsa1, groupkeys, group_funcs[group_func_name])
        else:
            logging.info("groupkeys is None, fallback to get ungrouped tsa")
            tsa = datalogger.load_tsa(datestring, index_pattern=index_pattern)
        logging.info(tsa.keys()[0])
        web.header('Content-type', 'text/html')
        # you must not set this option, according to
        # http://stackoverflow.com/questions/11866333/ioerror-when-trying-to-serve-file
        # web.header('Transfer-Encoding','chunked')
        yield "[" + json.dumps(tsa.export().next())
        for chunk in tsa.export():
            #logging.info("yielding %s", chunk)
            yield "," + json.dumps(chunk)
        yield "]"

    def get_ts(self, project, tablename, datestring, args):
        """
        get TimeseriesArray object with one particular Timeseries selected by key

        parameters:
        /<str>project/<str>tablename/<str>datestring/base64endoded(tuple(key))

        returns:
        tsa exported in JSON format
        """
        key_str = args[0]
        logging.info("base64encoded index_key: %s", key_str)
        key = tuple([unicode(key_value) for key_value in eval(base64.b64decode(key_str))])
        logging.info("project : %s", project)
        logging.info("tablename : %s", tablename)
        logging.info("datestring : %s", datestring)
        logging.info("key : %s", key)
        datalogger = DataLogger(basedir, project, tablename)
        key_dict = dict(zip(datalogger.index_keynames, key))
        tsa = datalogger.load_tsa(datestring, filterkeys=key_dict)
        yield "[" + json.dumps(tsa.export().next())
        for chunk in tsa.export():
            yield "," + json.dumps(chunk)
        yield "]"
        #outbuffer = json.dumps(tuple(tsa.export()))
        #return outbuffer

    def get_tsastats(self, project, tablename, datestring, args):
        """
        return exported TimeseriesArrayStats json formatted

        [
            list of index_keys,
            list of value_keys,
            list of [
                index_key : tsstat_dictionary
                ]
        ]

        if optional args is given, only one specific statistical function is returned

        returns:
        json(tsastats_dict)
        """
        logging.info("optional arguments received: %s", args)
        datalogger = DataLogger(basedir, project, tablename)
        tsastats = datalogger.load_tsastats(datestring)
        if len(args) > 0:
            return json.dumps(tsastats.to_csv(args[0]))
        return tsastats.to_json()

    def get_quantile(self, project, tablename, datestring, args):
        """
        return exported QuantileArray json formatted

        ex: Datalogger/get_quantile/{projectname}/{tablename}/{datestring}

        [
            dict of index_keys : dict of quantile,
            list of index_keys,
            list of value_names,
        ]

        returns:
        json(quantile_dict)
        """
        logging.info("optional arguments received: %s", args)
        datalogger = DataLogger(basedir, project, tablename)
        quantiles = datalogger.load_quantile(datestring)
        if len(args) > 0:
            value_keyname = args[0]
            ret_data = []
            # build header
            ret_data.append(list(datalogger.index_keynames) + ["Q0", "Q1", "Q2", "Q3", "Q4"])
            # data part
            for k, v  in quantile[value_keyname].quantile.items():
                ret_data.append(list(k) + v.values())
            return json.dumps(ret_data)
        return quantile.to_json()

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
        # key_str should be a tuple string, convert to unicode tuple
        keys = tuple([unicode(key_value) for key_value in eval(base64.b64decode(keys_str))])
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
        datalogger = DataLogger(basedir, project, tablename)
        keys_dict = dict(zip(datalogger.index_keynames, keys))
        # build filter if any group_by is given
        filterkeys = keys_dict # default
        if len(group_by) > 0:
            filterkeys = {}
            for key in group_by:
                filterkeys[key] = keys_dict[key]
        logging.info("useing filterkeys: %s", filterkeys)
        tsa = datalogger.load_tsa(datestring, filterkeys=filterkeys)
        logging.info("got tsa with %d keys", len(tsa))
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
        stats = None
        if len(group_by) > 0:
            logging.info("generating new key for left possible keys in grouped tsa")
            key_dict = dict(zip(datalogger.index_keynames, keys))
            new_key = tuple((key_dict[key] for key in group_by))
            logging.info("key after grouping would be %s", new_key)
            logging.info("grouping tsa by %s", group_by)
            new_tsa = datalogger.group_by(datestring, tsa, group_by, group_func=lambda a, b: a + b)
            #new_tsa = tsa.get_group_by_tsa(group_by, group_func=lambda a: sum(a))
            tsa = new_tsa
            data = tsa[new_key].dump_dict()
            stats = tsa[new_key].stats.htmltable()
        else:
            data = tsa[keys].dump_dict()
            stats = tsa[keys].stats.htmltable()
        result = {
                "stats" : stats,
                "data" : [],
                }
        # holds return data
        logging.info("data keys : %s", data[data.keys()[0]].keys())
        for value_key in value_keys:
            # ist important to sort by timestamp, to not confuse
            # highcharts
            result["data"].append(
                {
                    "name" : value_key,
                    "data" : tuple(((ts * 1000, row_dict[value_key]) for ts, row_dict in sorted(data.items())))
                }
            )
        return json.dumps(result)

    def get_hc_daily_data(self, args):
        """
        get values(min 1) from TimeseriesArray to use for highcharts graphing

        parameters:
        /project/tablename/datestring/index_key/value_keynames/index_keyname

        <b>poject</b> <str> defines which project to use
        <b>tablename</b> <str> defines which tablename to use
        <b>datestring</b> <str> in form of YYYY-MM-DD to define whih day to use
        <b>index_key</b> base64 encoded tuple, defines which Timeseries to use, ex. (u'srvcl14db2.tilak.cc', u'DB2', u'ablagsys', u'data only')
        <b>value_keynames</b> json encoded list of value_keynames to show in graph
            each value_keyname will be a separate highchart line
        <b>index_keynam</b> json encoded <str> or null
            if given, the data will be grouped on this given index_keyname
            if hostname is given the above example will be gruped by hostname=u'srvcl14db2.tilak.cc'
            and all possible Timeseries will be summed up

        return data json encoded like this
        [
            {   name : "timeseries value_name 1",
                data : [[ts, value], ...]
            },
            {   name : "timeseries value name 2",
                data : [[ts, value], ...]
            }
            ...
        ]
        this structure could already be used in highcharts.data
        """
        assert len(args) == 6
        project, tablename, datestring, index_key_b64, value_keynames_str, index_keyname_str = args
        # key_str should be a tuple string, convert to unicode tuple
        index_key = tuple([unicode(key_value) for key_value in eval(base64.b64decode(index_key_b64))])
        value_keynames = ()
        if json.loads(value_keynames_str) is not None:
            value_keynames = tuple(json.loads(value_keynames_str))
        index_keyname = ()
        if json.loads(index_keyname_str) is not None:
            index_keyname = (json.loads(index_keyname_str),)
        logging.info("project : %s", project)
        logging.info("tablename : %s", tablename)
        logging.info("datestring : %s", datestring)
        logging.info("index_key : %s", index_key)
        logging.info("value_keynames : %s", value_keynames)
        logging.info("index_keyname : %s", index_keyname)
        datalogger = DataLogger(basedir, project, tablename)
        index_key_dict = dict(zip(datalogger.index_keynames, index_key))
        # build filter if any group_by is given
        filterkeys = index_key_dict # default
        if len(index_keyname) > 0:
            filterkeys = {}
            for key in index_keyname:
                filterkeys[key] = index_key_dict[key]
        logging.info("using filterkeys: %s", filterkeys)
        tsa = datalogger.load_tsa(datestring, filterkeys=filterkeys)
        logging.info("got tsa with %d keys", len(tsa))
        # grouping stuff if necessary
        data = None # holds finally calculated data
        stats = None # holds tsstats informations
        if len(index_keyname) > 0:
            # grouping by key named
            logging.info("generating new key for left possible keys in grouped tsa")
            new_key = tuple((index_key_dict[key] for key in index_keyname))
            logging.info("key after grouping would be %s", new_key)
            logging.info("grouping tsa by %s", index_keyname)
            new_tsa = datalogger.group_by(datestring, tsa, index_keyname, group_func=lambda a, b: a + b)
            tsa = new_tsa
            data = tsa[new_key].dump_dict()
            stats = tsa[new_key].stats.get_stats()
        else:
            # not grouping, simple
            data = tsa[index_key].dump_dict()
            stats = tsa[index_key].stats.get_stats()
        # holds return data
        logging.info("data keys : %s", data[data.keys()[0]].keys())
        # get in highcharts shape
        result = {
            "stats" : stats,
            "data" : [], # holds highchart data
        }
        for value_keyname in value_keynames:
            # its important to sort by timestamp, to not confuse
            # highcharts
            result["data"].append(
                {
                    "name" : value_keyname,
                    "data" : tuple(((ts * 1000, row_dict[value_keyname]) for ts, row_dict in sorted(data.items())))
                }
            )
        return json.dumps(result)

    def get_longtime_data(self, args):
        """
        get values from RAW Archive

        parameters:
        /<str>project/<str>tablename/<str>datestring/<str>key/<str>value_keys

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
        assert len(args) == 5
        project, tablename, monthstring, keys_str, value_key = args
        if len(monthstring) > 7:
            return "monthstring, has to be in YYYY-MM format"
        # key_str should be a tuple string, convert to unicode tuple
        keys = tuple([unicode(key_value) for key_value in eval(base64.b64decode(keys_str))])
        logging.info("project : %s", project)
        logging.info("tablename : %s", tablename)
        logging.info("monthstring : %s", monthstring)
        logging.info("keys : %s", keys)
        logging.info("value_keys : %s", value_key)
        datalogger = DataLogger(basedir, project, tablename)
        data = datalogger.get_tsastats_longtime_hc(monthstring, keys, value_key)
        #logging.info("got data: %s", data)
        hc_data = [{"name" : funcname, "data" : data[funcname]} for funcname in data.keys()]
        return json.dumps(hc_data)

    def upload_raw_file(self, args):
        """
        save receiving file into datalogger structure

        /project/tablename/datestring
        """
        assert len(args) == 3
        project, tablename, datestring = args
        logging.info("basedir:   %s", basedir)
        logging.info("tablename: %s", tablename)
        logging.info("datestring:%s", datestring)
        datalogger = DataLogger(basedir, project, tablename)
        filename = os.path.join(datalogger.raw_basedir, "%s_%s.csv.gz" % (tablename, datestring))
        if os.path.isfile(filename):
            logging.info("File already exists")
            return "File already exists"
        try:
            filehandle = gzip.open(filename, "wb")
            x = web.input(myfile={})
            logging.info(x.keys())
            logging.info("Storing data to %s", filename)
            if "filedata" in x: # curl type
                filehandle.write(x["filedata"])
            else: # requests or urllib3 type
                filehandle.write(x["myfile"].file.read())
            filehandle.close()
        except StandardError as exc:
            logging.exception(exc)
            os.unlink(filename)
            logging.info("Error while saving received data to")
            return "Error while saving received data to"
        try:
            tsa = datalogger[str(datestring)] # read received data
        except StandardError as exc:
            logging.exception(exc)
            os.unlink(filename)
            logging.info("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
            return "Invalid data in uploaded file, see apache error log for details, uploaded file not stored"
        logging.info("File stored")
        return "File stored"

    @staticmethod
    @calllogger
    def get_scatter_data(args):
        """
        gets scatter plot data of two value_keys of the same tablename

        ex: Datalogger/{projectname}/{tablename}/{datestring}/{value_keyname1}/{value_keyname2}/{stat function name}

        value_keyname{1/2} has to be one of get_value_keynames
        stat function name has to be one of get_stat_func_names

        returns:
        json(highgraph data)
        """
        assert len(args) == 6
        project, tablename, datestring, value_key1, value_key2, stat_func_name = args
        logging.info("project : %s", project)
        logging.info("tablename : %s", tablename)
        logging.info("datestring : %s", datestring)
        logging.info("value_key1 : %s", value_key1)
        logging.info("value_key2 : %s", value_key2)
        datalogger = DataLogger(basedir, project, tablename)
        tsastats = datalogger.load_tsastats(datestring)
        hc_scatter_data = []
        for key, tsstat in tsastats.items():
            hc_scatter_data.append({
                "name" : str(key),
                "data" : ((tsstat[value_key1]["avg"], tsstat[value_key2]["avg"]), )
            })
        return json.dumps(hc_scatter_data)

    @staticmethod
    def sr_vicenter_unused_cpu_cores(args):
        """
        special report to find virtual machine which re not used their virtual core entirely
        on this machine there is a possibility to save some virtual cores

        works only for VMware machines, in special virtualMachineCpuStats
        """
        datestring = args[0]
        datalogger = DataLogger(basedir, "vicenter", "virtualMachineCpuStats")
        tsastat = datalogger.load_tsastats(datestring)
        tsastat_g = datalogger.tsastat_group_by(tsastat, ("hostname", ))
        data = []
        data.append(("hostname", "avg_idle_min", "avg_used_avg", "avg_used_max"))
        for key in tsastat_g.keys():
            num_cpu = sum([key[0] in index_key for index_key in tsastat.keys()])
            if num_cpu < 3:
                continue
            data.append((key[0], "%0.2f" % tsastat_g[key]["cpu.idle.summation"]["min"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["avg"], "%0.2f" % tsastat_g[key]["cpu.used.summation"]["max"]))
        return json.dumps(data)

    @staticmethod
    def sr_vicenter_unused_mem(args):
        """
        special resport to find virtual machine which are not used their ram entirely
        on this machines there is a possibility to save some virtual memory

        works only for VMware machine, in special virtualMachineMemoryStats
        """
        datestring = args[0]
        datalogger = DataLogger(basedir, "vicenter", "virtualMachineMemoryStats")
        tsastat = datalogger.load_tsastats(datestring)
        tsastat_g = datalogger.tsastat_group_by(tsastat, ("hostname", ))
        data = []
        data.append(("hostname", "avg_active_max", "avg_granted_min", "avg_notused_min"))
        for key in tsastat_g.keys():
            not_used = tsastat_g[key]["mem.granted.average"]["min"] - tsastat_g[key]["mem.active.average"]["max"]
            data.append((key[0], "%0.2f" % tsastat_g[key]["mem.active.average"]["max"], "%0.3f" % tsastat_g[key]["mem.granted.average"]["min"], "%0.2f" % not_used))
        return json.dumps(data)

    @staticmethod
    def sr_hrstorageram_unused(args):
        """
        special report to find servers which are not using their ram entirely
        specially on virtual machines are is a huge saving potential

        works only for snmp data especially hrStorageTable
        """
        datestring = args[0]
        datalogger = DataLogger(basedir, "snmp", "hrStorageTable")
        tsastat = datalogger.load_tsastats(datestring)
        data = []
        data.append(("hostname", "hrStorageSizeKb", "hrStorageUsedKb", "hrStorageNotUsedKbMin", "hrStorageNotUsedPct"))
        for index_key in tsastat.keys():
            # (u'srvcacdbp1.tilak.cc', u'Physical Memory',
            # u'HOST-RESOURCES-TYPES::hrStorageRam')
            if u'HOST-RESOURCES-TYPES::hrStorageRam' not in index_key:
                del tsastat[index_key]
        for key, tsstat in datalogger.tsastat_group_by(tsastat, ("hostname", )).items():
            sizekb = tsstat["hrStorageSize"]["min"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            usedkb = tsstat["hrStorageUsed"]["max"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            notused = sizekb - usedkb
            notused_pct = 100.0 *  notused / sizekb
            data.append((key[0], "%0.2f" % sizekb, "%0.2f" % usedkb, "%0.2f" % notused, "%0.2f" % notused_pct))
        return json.dumps(data)

    @staticmethod
    def sr_hrstorage_unused(args):
        """
        special report to get a report of unused SNMP Host Storage
        works only with snmp/hrStorageTable
        """
        datestring, storage_type = args[:2]
        datalogger = DataLogger(basedir, "snmp", "hrStorageTable")
        tsastat = datalogger.load_tsastats(datestring)
        data = []
        data.append(("hostname", "hrStorageDescr", "hrStorageSizeKb", "hrStorageUsedKb", "hrStorageNotUsedKbMin", "hrStorageNotUsedPct"))
        for index_key in tsastat.keys():
            # (u'srvcacdbp1.tilak.cc', u'Physical Memory',
            # u'HOST-RESOURCES-TYPES::hrStorageRam')
            if (u"HOST-RESOURCES-TYPES::%s" % storage_type) not in index_key:
                del tsastat[index_key]
            if index_key[1][:4] in (u"/run", u"/dev", u"/sys"):
                del tsastat[index_key]
        for key, tsstat in tsastat.items():
            sizekb = tsstat["hrStorageSize"]["min"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            usedkb = tsstat["hrStorageUsed"]["max"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            notused = sizekb - usedkb
            notused_pct = 0.0
            try:
                notused_pct = 100.0 *  notused / sizekb
            except ZeroDivisionError:
                pass
            data.append((key[0], key[1], "%0.2f" % sizekb, "%0.2f" % usedkb, "%0.2f" % notused, "%0.2f" % notused_pct))
        return json.dumps(data)


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()

