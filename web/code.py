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
        thiskey = unicode((func.__name__, args[1:], kwds))
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

        """
        method = args.split("/")[0].lower()
        logging.info("calling method %s", method)
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        web.header("Content-Type", "application/json")
#        web.header('Content-type', 'text/html')
        method_args = args.split("/")[1:] # all without method name
        method_func_dict = {
            "doc" : self.doc,
            "get_index_keynames" : self.get_index_keynames,
            "get_value_keynames" : self.get_value_keynames,
            "get_ts_keyname" : self.get_ts_keyname,
            "get_projects" : self.get_projects,
            "get_tablenames" : self.get_tablenames,
            "get_wikiname" : self.get_wikiname,
            "get_headers" : self.get_headers,
            "get_last_business_day_datestring" : self.get_last_business_day_datestring,
            "get_datewalk" : self.get_datewalk,
            "get_caches" : self.get_caches,
            "get_tsa" : self.get_tsa,
            "get_tsa_adv" : self.get_tsa_adv,
            "get_ts" : self.get_ts,
            "get_tsastats" : self.get_tsastats,
            "get_stat_func_names" : self.get_stat_func_names,
            "get_quantilles" : self.get_quantilles,
            "get_chart_data_ungrouped" : self.get_chart_data_ungrouped,
            "get_ts_caches" : self.get_ts_caches,
            "get_tsstat_caches" : self.get_tsstat_caches,
            "get_caches_dict" : self.get_caches_dict,
            "get_scatter_data" : self.get_scatter_data,
            "get_longtime_data" : self.get_longtime_data,
            "get_tsastats_table" : self.get_tsastats_table,
            "get_tsastats_func" : self.get_tsastats_func,
        }
        try:
            return method_func_dict[method](method_args)
        except KeyError as exc:
            logging.debug("unknown method called %s", method)
            return "There is no method called %s" % method

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
        """
        name = __name__
        doc = __doc__
        if len(args) == 1:
            func = eval("self.%s" % args[0])
            doc = func.__doc__
            name = func.__name__
        outbuffer = ["def %s(*args, **kwds)" % name]
        if doc is not None:
            outbuffer += doc.split("\n")
        return "<br>".join(outbuffer)

    @memcache
    def get_projects(self, args):
        """
        get all available projects, use directory listing for that,
        but blacklist some non project directories

        parameters:
        None

        returns:
        <json><list> of existing project names
        """
        return json.dumps(DataLogger.get_projects(basedir))

    @memcache
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
        return json.dumps(DataLogger.get_tablenames(basedir, project))

    @memcache
    def get_wikiname(self, args):
        """
        return WikiName for given project/tablename

        parameters:
        <str>projectname
        <str>tablename

        returns:
        <json><str> to use as WikiName
        """
        assert len(args) == 2
        project, tablename = args
        return json.dumps("DataLoggerReport%s%s" % (project.capitalize(), tablename.capitalize()))

    @memcache
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
        return json.dumps(datalogger.headers)

    @memcache
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
        return json.dumps(datalogger.index_keynames)

    @memcache
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
        return json.dumps(datalogger.value_keynames)

    @memcache
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
        return json.dumps(datalogger.ts_keyname)

    @memcache
    def get_ts_caches(self, args):
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
        keys = []
        for cache in datalogger.list_ts_caches(datestring):
            keys.append(cache[1])
        return json.dumps(keys)

    @memcache
    def get_tsstat_caches(self, args):
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
        keys = []
        for cache in datalogger.list_tsstat_caches(datestring):
            keys.append(cache[1])
        return json.dumps(keys)

    @memcache
    def get_caches_dict(self, args):
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
        keys = []
        for cache in datalogger.list_ts_caches(datestring):
            key = dict(zip(datalogger.index_keynames, cache[1][1]))
            keys.append(key)
        return json.dumps(keys)

    @memcache
    def get_last_business_day_datestring(self, args):
        """
        get datestring of last businessday Mo.-Fr.
        """
        return json.dumps(DataLogger.get_last_business_day_datestring())

    @memcache
    def get_datewalk(self, args):
        """
        get datestring of last businessday Mo.-Fr.
        """
        datestring1, datestring2 = args
        data = tuple(DataLogger.datewalker(datestring1, datestring2))
        return json.dumps(data)

    @memcache
    def get_caches(self, args):
        """
        return dictionary of caches available for this project/tablename/datestring combination
        """
        project, tablename, datestring = args
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

    def get_ts(self, args):
        """
        get TimeseriesArray object with one particular Timeseries selected by key

        parameters:
        /<str>project/<str>tablename/<str>datestring/base64endoded(tuple(key))

        returns:
        tsa exported in JSON format
        """
        assert len(args) == 4
        project, tablename, datestring, key_str = args
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

    def get_tsastats(self, args):
        """
        return exported TimeseriesArrayStats json formatted
        """
        project, tablename, datestring = args
        datalogger = DataLogger(basedir, project, tablename)
        tsastats = datalogger.load_tsastats(datestring)
        return tsastats.to_json()

    @memcache
    def get_stat_func_names(self, args):
        """
        return defined stat_func_names in TimeseriesStats objects
        """
        stat_func_names = TimeseriesStats.stat_funcs.keys()
        return json.dumps(stat_func_names)

    def get_quantilles(self, args):
        """
        return exported QuantillesArray json formatted
        """
        project, tablename, datestring = args
        datalogger = DataLogger(basedir, project, tablename)
        quantilles = datalogger.load_quantilles(datestring)
        return quantilles.to_json()

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

    def get_tsastats_table(self, args):
        """
        return exported QuantillesArray json formatted
        """
        def csv_to_table(csvdata, keys):
            outbuffer = []
            outbuffer.append("<thead><tr>")
            [outbuffer.append("<th>%s</th>" % header) for header in csvdata[0]]
            outbuffer.append("</tr></thead><tbody>")
            for values in csvdata[1:]:
                outbuffer.append("<tr>")
                [outbuffer.append("<td >%s</td>" % value) for value in values[0:keys]]
                [outbuffer.append("<td type=numeric>%0.2f</td>" % value) for value in values[keys:]]
                outbuffer.append("</tr>")
            outbuffer.append("</tbody>")
            return outbuffer
        project, tablename, datestring, stat_func_name = args
        datalogger = DataLogger(basedir, project, tablename)
        tsastats = datalogger.load_tsastats(datestring)
        return json.dumps("\n".join(csv_to_table(tsastats.to_csv(stat_func_name), len(tsastats.index_keys))))

    def get_tsastats_func(self, args):
        """
        return jason data to render html table from it
        """
        def csv_to_table(csvdata, keys):
            outbuffer = []
            outbuffer.append("<thead><tr>")
            [outbuffer.append("<th>%s</th>" % header) for header in csvdata[0]]
            outbuffer.append("</tr></thead><tbody>")
            for values in csvdata[1:]:
                outbuffer.append("<tr>")
                [outbuffer.append("<td >%s</td>" % value) for value in values[0:keys]]
                [outbuffer.append("<td type=numeric>%0.2f</td>" % value) for value in values[keys:]]
                outbuffer.append("</tr>")
            outbuffer.append("</tbody>")
            return outbuffer
        project, tablename, datestring, stat_func_name = args
        datalogger = DataLogger(basedir, project, tablename)
        tsastats = datalogger.load_tsastats(datestring)
        return json.dumps(tsastats.to_csv(stat_func_name))

    @staticmethod
    def get_scatter_data(args):
        """
        gets scatter plot data

        vicenter/hostSystemDiskStats/2015-07-13/disk.totalReadLatency.average/disk.totalWriteLatency.average/avg
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

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()

