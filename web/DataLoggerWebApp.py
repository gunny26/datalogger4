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
import tk_web
from datalogger import DataLoggerRawFileMissing as DataLoggerRawFileMissing
from datalogger import DataLoggerLiveDataError as DataLoggerLiveDataError
from datalogger import DataLogger as DataLogger
from datalogger import TimeseriesStats as TimeseriesStats

urls = (
    "/oauth2/v1/", "tk_web.IdpConnector",
    "/(.*)", "DataLoggerWebApp",
    )

application = web.application(urls, globals()).wsgifunc()

CONFIG = tk_web.TkWebConfig("~/DataLoggerWebApp.json")
basedir = CONFIG["BASEDIR"]
# prepare IDP Connector to use actual CONFIG
tk_web.IdpConnector.CONFIG = CONFIG
tk_web.IdpConnector.web = web
# some decorators, use it as needed
authenticator = tk_web.std_authenticator(web, CONFIG)
calllogger = tk_web.std_calllogger(web, CONFIG)
outformat = tk_web.std_jsonout(web, CONFIG)

#handler = logging.handlers.RotatingFileHandler(
#    os.path.join(basedir, "/var/log/apache2/datalogger.log"),
#    maxBytes=10 * 1024 * 1024,
#    backupCount=5)
#logging.getLogger("").addHandler(handler)
#logging.getLogger("").setLevel(level=logging.DEBUG)

#def calllogger(func):
#    """
#    decorator to log and measure call durations
#    """
#    def inner(*args, **kwds):
#        starttime = time.time()
#        call_str = "%s(%s, %s)" % (func.__name__, args, kwds)
#        logging.debug("calling %s", call_str)
#        try:
#            ret_val = func(*args, **kwds)
#            logging.debug("duration of call %s : %s", call_str, (time.time() - starttime))
#            return ret_val
#        except StandardError as exc:
#            logging.exception(exc)
#            logging.error("call to %s caused StandardError", call_str)
#            return "call to %s caused StandardError" % call_str
#    # set inner function __name__ and __doc__ to original ones
#    inner.__name__ = func.__name__
#    inner.__doc__ = func.__doc__
#    return inner

MEMCACHE = {}
MAXAGE = 300
def memcache(func):
    """
    decorator to cache return values according to used function parameters
    """
    logger = logging.getLogger("MemCache")
    def inner(*args, **kwds):
        starttime = time.time()
        thiskey = unicode((func.__name__, args, kwds))
        logger.info("number of keys in cache %d", len(MEMCACHE.keys()))
        logger.info("key to look for %s", thiskey)
        # get rid of old cache entries
        for key in MEMCACHE.keys():
            if (MEMCACHE[key]["ts"] + MAXAGE) < starttime:
                logger.info("deleting aged cache entry for key %s", key)
                try:
                    del MEMCACHE[key]
                except KeyError:
                    pass # ignore race conditioned KeyError
        # is there an entry for this key
        if thiskey in MEMCACHE:
            if (MEMCACHE[thiskey]["ts"] + MAXAGE) > starttime:
                logger.info("returning from cache for key %s", thiskey)
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
            logger.exception(exc)
    # set inner function __name__ and __doc__ to original ones
    inner.__name__ = func.__name__
    inner.__doc__ = func.__doc__
    return inner



class DataLoggerWebApp(object):
    """retrieve Data from RRD Archive"""

    def __init__(self):
        """__init__"""
        self.logger = logging.getLogger("DataLoggerWebApp")

    def OPTIONS(self, args):
        self.logger.info("OPTIONS called")
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        web.header('Access-Control-Allow-Headers', 'x-authkey')
        web.header('Access-Control-Allow-Headers', 'x-apikey')

    @authenticator
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
        web.header("Content-Type", "application/json")
        self.logger.info("received args %s", args)
        if args == u"":
            return self.get_projects()
        # strip trailing slash
        if args[-1] == u"/":
            args = args[:-1]
        parameters = args.split("/")
        if len(parameters) > 0:
            self.logger.info("received parameters %s", parameters)
            if parameters[0] == "sr":
                # hook to implement Special Reports
                # htey usually dont need any parameters other than a
                # datestring
                # sr/<datestring>/<report-name>
                report_name = parameters[1]
                if report_name == "vicenter_unused_cpu_cores":
                    return self.sr_vicenter_unused_cpu_cores(parameters[2:])
                elif report_name == "vicenter_unused_mem":
                    return self.sr_vicenter_unused_mem(parameters[2:])
                elif report_name == "hrstorageram_unused":
                    return self.sr_hrstorageram_unused(parameters[2:])
                elif report_name == "hrstorage_unused":
                    return self.sr_hrstorage_unused(parameters[2:])
                else:
                    web.notfound()
                    return
            else:
                # ok this lookes like /<project-name>/...
                project = parameters[0]
                if len(parameters) == 1:
                    # ok this is /<project-name>
                    return self.get_tablenames(project)
                elif len(parameters) == 2:
                    # ok this is /<project-name>/<tablename>
                    tablename = parameters[1]
                    return self.get_meta([project, tablename])
                elif len(parameters) == 3:
                    # ok this is
                    # /<project-name>/<tablename>/<datestring>
                    tablename = parameters[1]
                    datestring = parameters[2]
                    return self.get_caches([project, tablename, datestring])
                elif len(parameters) >= 4:
                    # ok this looks like
                    # /<project-name>/<tablename>/<datestring>/...
                    tablename = parameters[1]
                    datestring = parameters[2]
                    function = parameters[3]
                    if len(datestring) == 7:
                        # datestring is a monthstring, only 7 charecters
                        # long
                        args = []
                        if len(parameters) > 4:
                            args = parameters[4:]
                        if function == "ts":
                           return self.get_monthly_ts(project, tablename, datestring, args)
                        else:
                            web.notfound()
                    elif len(datestring) == 10:
                        # valid datestring, 10 characters long
                        args = []
                        if len(parameters) > 4:
                            # cut the remaining arguments
                            args = parameters[4:]
                        if function == "tsa":
                            return self.get_tsa(project, tablename, datestring, args)
                        elif function == "ts":
                            return self.get_ts(project, tablename, datestring, args)
                        elif function == "tsstat":
                            return self.get_tsstat(project, tablename, datestring, args)
                        elif (function == "tsastats") or (function == "tsastat"):
                            return self.get_tsastats(project, tablename, datestring, args)
                        elif function == "quantile":
                            return self.get_quantile(project, tablename, datestring, args)
                        elif function == "scatter":
                            return self.get_scatter(project, tablename, datestring, args)
                        web.notfound()
                    elif datestring == "lt":
                        # longtime data
                        # TODO seems to be much overloaded
                        return self.get_lt_ts(project, tablename, parameters[3:])
                    else:
                        # crap, this should not happen
                        web.internalerror()
        else:
            self.logger.debug("unknown call %s", parameters)
            web.notfound()
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
        self.logger.info("method %s should be called", method)
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
            self.logger.info(exc)
            outbuffer.append(str(exc))
        outbuffer.append("</body></html>")
        return "\n".join(outbuffer)

    @memcache
    def get_projects(self):
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

    @memcache
    def get_tablenames(self, project):
        """
        get available tablenames, for one particular project
        uses directory listing in raw subdirectory for this purpose

        ex: Datalogger/get_tablenames/{projectname}
        <projectname> has to be something from Datalogger/get_projects

        returns:
        json(list of possible tablenames for given project)
        """
        return json.dumps(DataLogger.get_tablenames(basedir, project))

    @memcache
    def get_meta(self, args):
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

    def get_caches(self, args):
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
            self.logger.exception(exc)
            self.logger.error(caches)
        return json.dumps(caches)

    def get_tsa(self, project, tablename, datestring, args):
        """
        return exported TimeseriesArray json formatted
        """
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
        self.logger.info(args)
        project, tablename, datestring, groupkeys_enc, group_func_name, index_pattern_enc = args
        groupkeys_dec = eval(base64.b64decode(groupkeys_enc)) # should be tuple
        logging.info("groupkeys_dec: %s", groupkeys_dec)
        index_pattern = base64.b64decode(index_pattern_enc)
        if index_pattern == "None":
            index_pattern = None
        self.logger.info("index_pattern: %s", index_pattern)
        assert group_func_name in group_funcs.keys()
        datalogger = DataLogger(basedir, project, tablename)
        tsa = None
        # gete data
        if groupkeys_dec is not None:
            self.logger.info("groupkeys is %s", groupkeys_dec)
            groupkeys = tuple([unicode(key_value) for key_value in groupkeys_dec])
            tsa1 = datalogger.load_tsa(datestring, index_pattern=index_pattern)
            tsa = datalogger.group_by(datestring, tsa1, groupkeys, group_funcs[group_func_name])
        else:
            self.logger.info("groupkeys is None, fallback to get ungrouped tsa")
            tsa = datalogger.load_tsa(datestring, index_pattern=index_pattern)
        self.logger.info(tsa.keys()[0])
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
        self.logger.info("base64encoded index_key: %s", key_str)
        key = tuple([unicode(key_value) for key_value in eval(base64.b64decode(key_str))])
        self.logger.info("project : %s", project)
        self.logger.info("tablename : %s", tablename)
        self.logger.info("datestring : %s", datestring)
        self.logger.info("key : %s", key)
        datalogger = DataLogger(basedir, project, tablename)
        key_dict = dict(zip(datalogger.index_keynames, key))
        tsa = datalogger.load_tsa(datestring, filterkeys=key_dict)
        yield "[" + json.dumps(tsa.export().next())
        for chunk in tsa.export():
            yield "," + json.dumps(chunk)
        yield "]"
        #outbuffer = json.dumps(tuple(tsa.export()))
        #return outbuffer

    def get_tsstat(self, project, tablename, datestring, args):
        """
        return exported TimeseriesStats data

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
        self.logger.info("optional arguments received: %s", args)
        if len(args) > 0:
            key_str = args[0]
            key = tuple([unicode(key_value) for key_value in eval(base64.b64decode(key_str))])
            datalogger = DataLogger(basedir, project, tablename)
            tsastats = datalogger.load_tsastats(datestring)
            return json.dumps(tsastats[key].stats)

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
        self.logger.info("optional arguments received: %s", args)
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
        self.logger.info("optional arguments received: %s", args)
        datalogger = DataLogger(basedir, project, tablename)
        quantile = datalogger.load_quantile(datestring)
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

    def get_monthly_ts(self, project, tablename, monthstring, args):
        """
        get monthly statistical values

        TODO: should be combined with get_lt_ts
        """
        index_key_enc = None
        value_keyname = None
        stat_func_name = "avg"
        if len(args) == 2:
            index_key_enc, value_keyname = args
        else:
            index_key_enc, value_keyname, stat_func_name = args
        if len(monthstring) != 7:
            web.internalerror()
            return "monthstring, has to be in YYYY-MM format"
        # key_str should be a tuple string, convert to unicode tuple
        index_key = tuple([unicode(key_value) for key_value in eval(base64.b64decode(index_key_enc))])
        self.logger.info("index_key : %s", index_key)
        self.logger.info("value_keyname : %s", value_keyname)
        self.logger.info("stat_func_name: %s", stat_func_name)
        datalogger = DataLogger(basedir, project, tablename)
        filterkeys = dict(zip(datalogger.index_keynames, index_key))
        ret_data = []
        for datestring in datalogger.monthwalker(monthstring):
            self.logger.debug("getting tsatstats for %s", monthstring)
            try:
                tsastats = datalogger.load_tsastats(datestring, filterkeys=filterkeys)
                ret_data.append([datestring, tsastats[index_key][value_keyname][stat_func_name]])
            except DataLoggerRawFileMissing as exc:
                self.logger.error("No Input File for datestring %s found, skipping this date", datestring)
            except DataLoggerLiveDataError as exc:
                self.logger.error("Reading from live data is not allowed, skipping this data, and ending loop")
                break
        return json.dumps(ret_data)

    def get_lt_ts(self, project, tablename, args):
        """
        get longtime statistical values
        """
        # datestringStart + "/" + datestringStop + "/" + Base64.encode(indexKey) + "/" + valueKeyname + "/" + statFuncName
        start, stop, index_key_enc, value_keyname, stat_func_name = args
        index_key = tuple([unicode(key_value) for key_value in eval(base64.b64decode(index_key_enc))])
        datalogger = DataLogger(basedir, project, tablename)
        filterkeys = dict(zip(datalogger.index_keynames, index_key))
        ret_data = []
        for datestring in datalogger.datewalker(start, stop):
            try:
                tsastats = datalogger.load_tsastats(datestring, filterkeys=filterkeys)
                ret_data.append([datestring, tsastats[index_key][value_keyname][stat_func_name]])
            except DataLoggerRawFileMissing as exc:
                self.logger.error("No Input File for datestring %s found, skipping this date", datestring)
            except DataLoggerLiveDataError as exc:
                self.logger.error("Reading from live data is not allowed, skipping this data, and ending loop")
                break
        return json.dumps(ret_data)

    def upload_raw_file(self, args):
        """
        save receiving file into datalogger structure

        /project/tablename/datestring
        """
        assert len(args) == 3
        project, tablename, datestring = args
        self.logger.info("basedir:   %s", basedir)
        self.logger.info("tablename: %s", tablename)
        self.logger.info("datestring:%s", datestring)
        datalogger = DataLogger(basedir, project, tablename)
        filename = os.path.join(datalogger.raw_basedir, "%s_%s.csv.gz" % (tablename, datestring))
        if os.path.isfile(filename):
            self.logger.info("File already exists")
            return "File already exists"
        try:
            filehandle = gzip.open(filename, "wb")
            x = web.input(myfile={})
            self.logger.info(x.keys())
            self.logger.info("Storing data to %s", filename)
            if "filedata" in x: # curl type
                filehandle.write(x["filedata"])
            else: # requests or urllib3 type
                filehandle.write(x["myfile"].file.read())
            filehandle.close()
        except StandardError as exc:
            self.logger.exception(exc)
            os.unlink(filename)
            self.logger.info("Error while saving received data to")
            return "Error while saving received data to"
        try:
            tsa = datalogger[str(datestring)] # read received data
        except StandardError as exc:
            self.logger.exception(exc)
            os.unlink(filename)
            self.logger.info("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
            return "Invalid data in uploaded file, see apache error log for details, uploaded file not stored"
        self.logger.info("File stored")
        return "File stored"

    @calllogger
    def get_scatter(self, project, tablename, datestring, args):
        """
        gets scatter plot data of two value_keys of the same tablename

        ex: Datalogger/{projectname}/{tablename}/{datestring}/{value_keyname1}/{value_keyname2}/{stat function name}

        value_keyname{1/2} has to be one of get_value_keynames
        stat function name has to be one of get_stat_func_names

        returns:
        json(highgraph data)
        """
        value_key1, value_key2, stat_func_name = args
        self.logger.info("project : %s", project)
        self.logger.info("tablename : %s", tablename)
        self.logger.info("datestring : %s", datestring)
        self.logger.info("value_key1 : %s", value_key1)
        self.logger.info("value_key2 : %s", value_key2)
        datalogger = DataLogger(basedir, project, tablename)
        tsastats = datalogger.load_tsastats(datestring)
        hc_scatter_data = []
        for key, tsstat in tsastats.items():
            hc_scatter_data.append({
                "name" : str(key),
                "data" : ((tsstat[value_key1][stat_func_name], tsstat[value_key2][stat_func_name]), )
            })
        return json.dumps(hc_scatter_data)

    def sr_vicenter_unused_cpu_cores(self, args):
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

    def sr_vicenter_unused_mem(self, args):
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

    def sr_hrstorageram_unused(self, args):
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

    def sr_hrstorage_unused(self, args):
        """
        special report to get a report of unused SNMP Host Storage
        works only with snmp/hrStorageTable
        """
        datestring = args[0]
        storage_type = "hrStorageFixedDisk"
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

