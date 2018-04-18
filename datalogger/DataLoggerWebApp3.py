#!/usr/bin/python

import os
import logging
logging.basicConfig(level=logging.ERROR)
import json
import gzip
import base64
import web
# own modules
import tk_web
from CustomExceptions import *
from DataLogger import DataLogger as DataLogger
from TimeseriesStats import TimeseriesStats as TimeseriesStats

urls = (
    "/oauth2/v1/", "tk_web.IdpConnector",
    "/(.*)", "DataLoggerWebApp3",
    )

CONFIG = tk_web.TkWebConfig("testdata/DataLoggerWebApp3.json")

class DataLoggerWebApp3(object):
    """
    retrieve Data from DataLogger
    purpose of this Web REST interface is to be simple and only do the necessary thing
    more sophistuicated calculations should be done on higher level apis
    """

    def __init__(self):
        """__init__"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__dl = DataLogger(CONFIG["BASEDIR"])

    def OPTIONS(self, args):
        """
        for Browser CORS checks to work
        """
        self.logger.info("OPTIONS called")
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        web.header('Access-Control-Allow-Headers', 'x-authkey')
        web.header('Access-Control-Allow-Headers', 'x-apikey')

    #@authenticator
    def GET(self, parameters):
        """
        GET Multiplexer function, according to first argument in URL
        call this function, and resturn result to client

        parameters:
        /<str>function_name/...

        return:
        return function_name(what is left of arguments)

        /config/ -> return main datalogger configuration
        /projects/ -> return projects
        /tablenames/<projectname>/ -> return tablenames
        /meta/<projectname>/<tablename>/ -> return table specification (index_keynames, headers, value_keynames, ts_keyname)
        /tsa/<projectname>/<tablename>/<datestring> -> get TimeseriesArray of this datestring
        /ts/<projectname>/<tablename>/<datestring>/<index_key base64 encoded> -> get Timeseries for this index_key
        /ts/<projectname>/<tablename>/<datestring>/<index_key base64 encoded>/<value_keyname> -> get only this series of Timeseries
        /quantile/<projectname>/<tablename>/<datestring> -> get QuantileArray of this datestring
        /tsastat/<projectname>/<tablename>/<datestring> -> get TimeseriesArrayStats of this datestring
        /tsstat/<projectname>/<tablename>/<datestring>/<index_key base64 encoded> -> get TimeseriesStats for this index_key
        """
        web.header("Content-Type", "application/json")
        self.logger.info("calling %s", parameters)
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        args = parameters.strip("/").split("/")
        # build method name from url
        method = "get_%s" % args[0].lower()
        query = dict(web.input()) # get query as dict
        try:
            func = getattr(self, method)
        except AttributeError as exc:
            self.logger.error(exc)
            web.ctx.status = "405 unknown method"
            return
        # calling method, or AttributeError if not found
        try:
            return func(*args[1:], **query)
        except KeyError as exc:
            self.logger.error(exc)
            web.ctx.status = "404 %s" % exc
            return
        except IndexError as exc:
            self.logger.error(exc)
            web.ctx.status = "404 %s" % exc
            return

    def doc(self, *args, **kwds):
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

    #@outformat
    def get_projects(self, *args, **kwds):
        """
        get available projects for this Datalogger Server

        ex: Datalogger/get_projects/...
        there is no further argument needed

        returns:
        json(existing project names)
        """
        return self.__dl.get_projects()

    #@outformat
    def get_tablenames(self, *args, **kwds):
        """
        get available tablenames, for one particular project
        uses directory listing in raw subdirectory for this purpose

        ex: Datalogger/get_tablenames/{projectname}
        <projectname> has to be something from Datalogger/get_projects

        returns:
        json(list of possible tablenames for given project)
        """
        project = args[0]
        return self.__dl.get_tablenames(project)

    #@memcache
    def get_meta(self, *args, **kwds):
        """
        get available tablenames, for one particular project
        uses directory listing in raw subdirectory for this purpose

        ex: Datalogger/get_tablenames/{projectname}
        <projectname> has to be something from Datalogger/get_projects

        returns:
        json(list of possible tablenames for given project)
        """
        project, tablename = args[:2]
        self.__dl.setup(project, tablename, "1970-01-01")
        return self.__dl.meta

    #@outformat
    def get_caches(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        caches = self.__dl["caches"]
        return caches

    #@outformat
    def get_tsa(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return self.__dl["tsa"].to_data()

    #@outformat
    def get_tsastats(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return self.__dl["tsastats"].to_data()

    #@outformat
    def get_ts(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring, index_key_b64 = args[:4]
        self.__dl.setup(project, tablename, datestring)
        index_key = tuple(eval(base64.b64decode(index_key_b64)))
        if len(args) >= 5:
            value_keynames = args[4:]
            print(value_keynames)
            return list(self.__dl["tsa", index_key].to_data(value_keynames))
        else:
            return list(self.__dl["tsa", index_key].to_data())

    #@outformat
    def get_tsstats(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring, index_key_b64 = args[:4]
        self.__dl.setup(project, tablename, datestring)
        index_key = tuple(eval(base64.b64decode(index_key_b64)))
        return self.__dl["tsastats", index_key].to_data()

    #@outformat
    def get_total_stats(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return self.__dl["total_stats"]

    #@outformat
    def get_qa(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return self.__dl["qa"].to_data()
    get_quantile = get_qa

    def get_ts_for_datestring(self, *args, **kwds):
        datestring = args[0]
        return self.__dl.get_ts_for_datestring(datestring)

    def get_stat_func_names(self, *args, **kwds):
        return self.__dl.stat_func_names

    def get_yesterday_datestring(self, *args, **kwds):
        return self.__dl.get_yesterday_datestring()

    def get_last_business_day_datestring(self, *args, **kwds):
        return self.__dl.get_last_business_day_datestring()

    def POST(self, parameters):
        """
        GET Multiplexer function, according to first argument in URL
        call this function, and resturn result to client

        parameters:
        /<str>function_name/...

        return:
        return function_name(what is left of arguments)

        """
        self.logger.info("calling %s", parameters)
        args = parameters.strip("/").split("/")
        # build method name from url
        method = "post_%s" % args[0].lower()
        try:
            # calling method, or AttributeError if not found
            return getattr(self, method)(*args[1:], **query)
        except AttributeError as exc:
            self.logger.error(exc)
        web.ctx.status = "405 unknown method"

    def post_raw_file(self, *args, **kwds):
        """
        save receiving file into datalogger structure

        /project/tablename/datestring
        """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        filename = os.path.join(self.__dl.raw_basedir, "%s_%s.csv.gz" % (tablename, datestring))
        if os.path.isfile(filename):
            self.logger.info("File already exists")
            return "File already exists"
        try:
            with gzip.open(filename, "wt") as outfile:
                x = web.input(myfile={})
                self.logger.info(x.keys())
                self.logger.info("Storing data to %s", filename)
                if "filedata" in x: # curl type
                    outfile.write(x["filedata"])
                else: # requests or urllib3 type
                    outfile.write(x["myfile"].file.read())
        except Exception as exc:
            self.logger.exception(exc)
            os.unlink(filename)
            self.logger.info("Error while saving received data to")
            return "Error while saving received data to"
        try:
            tsa = self.__dl["tsa"] # re-read received data
            assert isinstance(tsa, TimeseriesArrayStats)
        except AssertionError as exc:
            self.logger.exception(exc)
            os.unlink(filename)
            self.logger.info("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
            return "Invalid data in uploaded file, see apache error log for details, uploaded file not stored"
        self.logger.info("File stored")
        return "File stored"

    def DELETE(self, parameters):
        """
        GET Multiplexer function, according to first argument in URL
        call this function, and resturn result to client

        parameters:
        /<str>function_name/...

        return:
        return function_name(what is left of arguments)

        """
        self.logger.info("calling %s", parameters)
        args = parameters.strip("/").split("/")
        # build method name from url
        method = "delete_%s" % args[0].lower()
        query = dict(web.input()) # get query as dict
        try:
            # calling method, or AttributeError if not found
            return getattr(self, method)(*args[1:], **query)
        except AttributeError as exc:
            self.logger.error(exc)
        web.ctx.status = "405 unknown method"

    def delete_caches(self, *args, **kwds):
        """
        delete all available caches for this specific entry
        """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        self.__dl.delete_caches()


def get_config_pre_processor(config):
    def inner(handle):
        print("config_pre_processor called")
        web.ctx.tk_config = config
        return handle()
    return inner

def get_json_post_processor():
    def inner(handle):
        print("json_post_processor called")
        result = handle()
        if result is not None:
            return json.dumps(result)
    return inner

def get_exception_post_processor():
    def inner(handle):
        print("exception_post_processor called")
        try:
            return handle()
        except KeyError as exc:
            web.ctx.status = "404 %s" % exc
        except IndexError as exc:
            web.ctx.status = "404 %s" % exc
        return "error"
    return inner


def test():
    dlw3 = DataLoggerWebApp3()
    print("testing GET /projects")
    projects = dlw3.get_projects()
    print(projects)
    assert 'sanportperf' in projects
    print("testing GET /tablenames/mysql")
    tablenames = dlw3.get_tablenames("mysql")
    print(tablenames)
    assert tablenames == ["performance"]
    print("testing GET /meta/mysql/performance")
    meta = dlw3.get_meta("mysql", "performance")
    print(json.dumps(meta, indent=4))
    assert meta['index_keynames'] == ('hostname',)
    print("testing GET /tsa/mysql/performance/2018-04-01")
    tsa = dlw3.get_tsa("mysql", "performance", "2018-04-01")
    print(json.dumps(tsa, indent=4))
    assert "ts_KHUnbmFnaW9zLnRpbGFrLmNjJywp.csv.gz" in tsa["ts_filenames"]
    print("testing GET /tsastats/mysql/performance/2018-04-01")
    tsastats = dlw3.get_tsastats("mysql", "performance", "2018-04-01")
    print(json.dumps(tsastats, indent=4))
    assert "tsstat_KHUnbmFnaW9zLnRpbGFrLmNjJywp.json" in dlw3.get_tsastats("mysql", "performance", "2018-04-01")["tsstats_filenames"]
    print("testing GET /total_stats/mysql/performance/2018-04-01")
    total_stats = dlw3.get_total_stats("mysql", "performance", "2018-04-01")
    print(json.dumps(total_stats, indent=4))
    assert total_stats["bytes_sent"]["count"] == 1440.0
    print("testing GET /ts/mysql/performance/2018-04-01/KHUnbmFnaW9zLnRpbGFrLmNjJywp")
    ts = dlw3.get_ts("mysql", "performance", "2018-04-01", "KHUnbmFnaW9zLnRpbGFrLmNjJywp")
    print(json.dumps(ts, indent=4))
    assert ts[-1]["ts"] == 1522619702.0
    print("testing GET /ts/mysql/performance/2018-04-01/KHUnbmFnaW9zLnRpbGFrLmNjJywp/com_select/com_update")
    ts = dlw3.get_ts("mysql", "performance", "2018-04-01", "KHUnbmFnaW9zLnRpbGFrLmNjJywp", "com_select", "com_update")
    print(json.dumps(ts, indent=4))
    assert ts[-1]["com_select"] == 2.07
    print("testing GET /tsstats/mysql/performance/2018-04-01/KHUnbmFnaW9zLnRpbGFrLmNjJywp")
    tsstats = dlw3.get_tsstats("mysql", "performance", "2018-04-01", "KHUnbmFnaW9zLnRpbGFrLmNjJywp")
    print(json.dumps(tsstats, indent=4))
    assert tsstats["questions"]["diff"] == 55.57
    print("testing GET /qa/mysql/performance/2018-04-01")
    qa = dlw3.get_qa("mysql", "performance", "2018-04-01")
    print(json.dumps(qa, indent=4))
    print("testing GET /quantile/mysql/performance/2018-04-01/com_select")
    quantile = dlw3.get_quantile("mysql", "performance", "2018-04-01", "com_select")
    print(json.dumps(quantile, indent=4))
    print("testing GET /caches/mysql/performance/2018-04-01")
    caches = dlw3.get_caches("mysql", "performance", "2018-04-01")
    print(json.dumps(caches, indent=4))
    print("testing DELETE /caches/mysql/performance/2018-04-01")
    res = dlw3.delete_caches("mysql", "performance", "2018-04-01")
    assert res is None
    print("testing GET /caches/mysql/performance/2018-04-01")
    caches = dlw3.get_caches("mysql", "performance", "2018-04-01")
    print(json.dumps(caches, indent=4))
    assert caches["tsa"]["keys"] == {}
    print("testing GET /ts_for_datestring/2018-04-01")
    start, stop = dlw3.get_ts_for_datestring("2018-04-01")
    assert int(start) == 1522533599
    assert int(stop) == 1522619998
    print("testing GET /yesterday_datestring/")
    datestring = dlw3.get_yesterday_datestring()
    print(datestring)
    print("testing GET /last_business_day_datestring/")
    datestring = dlw3.get_last_business_day_datestring()
    print(datestring)


if __name__ == "__main__":
    # TESTING only
    # prepare IDP Connector to use actual CONFIG
    #tk_web.IdpConnector.CONFIG = CONFIG
    #tk_web.IdpConnector.web = web
    # some decorators, use it as needed
    #authenticator = tk_web.std_authenticator(web, CONFIG)
    #outformat = tk_web.std_jsonout(web, CONFIG)
    app = web.application(urls, globals())
    app.add_processor(get_exception_post_processor())
    app.add_processor(get_config_pre_processor(CONFIG))
    app.add_processor(get_json_post_processor())
    app.run() # will start local webserver
else:
    # if called over WSGI Interface
    CONFIG = tk_web.TkWebConfig("testdata/DataLoggerWebApp3.json")
    basedir = CONFIG["BASEDIR"]
    # prepare IDP Connector to use actual CONFIG
    tk_web.IdpConnector.CONFIG = CONFIG
    tk_web.IdpConnector.web = web
    # some decorators, use it as needed
    # authenticator = tk_web.std_authenticator(web, CONFIG)
    #outformat = tk_web.std_jsonout(web, CONFIG)
    #app = web.application(urls, globals())
    #app.add_processor(get_config_pre_processor(CONFIG))
    #app.add_processor(get_json_post_processor())
    #app.wsgifunc()
