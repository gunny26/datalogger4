#!/usr/bin/python

import os
import logging
logging.basicConfig(level=logging.INFO)
import json
import gzip
import datetime
import time
import web
# own modules
import tk_web
from datalogger3.CustomExceptions import *
from datalogger3.DataLogger import DataLogger as DataLogger
from datalogger3.TimeseriesStats import TimeseriesStats as TimeseriesStats
from datalogger3.b64 import b64eval

urls = (
    "/oauth2/v1/", "tk_web.IdpConnector",
    "/(.*)", "DataLoggerWebApp3",
    )

CONFIG = tk_web.TkWebConfig("/var/www/DataLoggerWebApp.json")
# prepare IDP Connector to use actual CONFIG
tk_web.IdpConnector.CONFIG = CONFIG
tk_web.IdpConnector.web = web
authenticator = tk_web.std_authenticator(web, CONFIG)
jsonout = tk_web.std_jsonout(web, CONFIG)


class DataLoggerWebApp3(object):
    """
    retrieve Data from DataLogger
    purpose of this Web REST interface is to be simple and only do the necessary thing
    more sophistuicated calculations should be done on higher level apis
    """

    __dl = DataLogger(CONFIG["BASEDIR"])
    logger = logging.getLogger("DataLoggerWebApp3")

    def OPTIONS(self, args):
        """
        for Browser CORS checks to work
        """
        self.logger.info("OPTIONS called")
        web.header('Access-Control-Allow-Origin', '*')
        web.header('Access-Control-Allow-Credentials', 'true')
        web.header('Access-Control-Allow-Headers', 'x-authkey')
        web.header('Access-Control-Allow-Headers', 'x-apikey')

    @authenticator
    @jsonout
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
        self.logger.info("GET calling %s", parameters)
        web.header("Content-Type", "application/json")
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
        except DataLoggerRawFileMissing as exc:
            web.ctx.status = "404 %s" % exc
            return
        except KeyError as exc:
            self.logger.exception(exc)
            self.logger.error(exc)
            web.ctx.status = "404 %s" % exc
            return
        except IndexError as exc:
            self.logger.exception(exc)
            self.logger.error(exc)
            web.ctx.status = "404 %s" % exc
            return

    def get_projects(self, *args, **kwds):
        """
        get available projects for this Datalogger Server

        ex: Datalogger/get_projects/...
        there is no further argument needed

        returns:
        json(existing project names)
        """
        return self.__dl.get_projects()

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

    def get_value_keynames(self, *args, **kwds):
        """
        get value_keynames
        """
        project, tablename = args[:2]
        if len(args) == 3:
            self.__dl.setup(project, tablename, args[2])
        else:
            self.__dl.setup(project, tablename, "1970-01-01")
        return self.__dl.value_keynames

    def get_index_keynames(self, *args, **kwds):
        """
        get index_keynames
        """
        project, tablename = args[:2]
        if len(args) == 3:
            self.__dl.setup(project, tablename, args[2])
        else:
            self.__dl.setup(project, tablename, "1970-01-01")
        return self.__dl.index_keynames

    def get_caches(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        caches = self.__dl["caches"]
        return caches

    def get_tsa(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return self.__dl["tsa"].to_data()

    def get_tsa_keys(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return [str(tuple(key)) for key in self.__dl["tsa"].keys()]

    def get_tsastats(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return self.__dl["tsastats"].to_data()

    def get_ts(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring, index_key_b64 = args[:4]
        self.__dl.setup(project, tablename, datestring)
        index_key = b64eval(index_key_b64)
        if len(args) >= 5:
            value_keynames = args[4:]
            print(value_keynames)
            return list(self.__dl["tsa", index_key].to_data(value_keynames))
        else:
            return list(self.__dl["tsa", index_key].to_data())

    def get_tsstats(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring, index_key_b64 = args[:4]
        self.__dl.setup(project, tablename, datestring)
        index_key = b64eval(index_key_b64)
        return self.__dl["tsastats", index_key].to_data()

    def get_total_stats(self, *args, **kwds):
        """ using DataLogger method """
        project, tablename, datestring = args[:3]
        self.__dl.setup(project, tablename, datestring)
        return self.__dl["total_stats"]

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

    ################## special JS WebApp Methods starting here ############################

    def get_js_tsastats(self, *args, **kwds):
        """
        get tsastat for one particular date and value_keyname

        project/tablename/datestring/value_keyname/stat_func_name

        either value_keyname or stat_func_name could be null,
        but not both

        if value_keyname is given return every stat_func_value
        if stat_func_name is given return every value_keyname
        """
        project, tablename, datestring, value_keyname, stat_func_name = args
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
            self.__dl.setup(project, tablename, datestring) # datestring is not important
            if value_keyname != "null":
                assert value_keyname in self.__dl.value_keynames
                stat_func_name = None
            elif stat_func_name != "null":
                assert stat_func_name in self.__dl.stat_func_names
                value_keyname = None
            assert value_keyname != stat_func_name
        except AssertionError as exc:
            self.logger.exception(exc)
            raise web.internalerror("Argument error")
        # get differential data
        data = self.__dl["tsastats"]
        # get only one specific value_keyname and row dictionaries
        # so this will be 2-dimensional
        rows = []
        if value_keyname is not None:
            headers = list(self.__dl.index_keynames) + ["value_keyname",] + list(self.__dl.stat_func_names)
            for index_key, stat_value in data.items():
                row_dict = dict(zip(self.__dl.index_keynames, index_key))
                row_dict["value_keyname"] = value_keyname
                row_dict.update(stat_value[value_keyname])
                rows.append([row_dict[key] for key in headers])
        elif stat_func_name is not None:
            headers = list(self.__dl.index_keynames) + ["stat_func_name",] + list(self.__dl.value_keynames)
            for index_key, stat_value in data.items():
                row_dict = dict(zip(self.__dl.index_keynames, index_key))
                row_dict["stat_func_name"] = stat_func_name
                row_dict.update(dict(((value_key, stat_value[value_key][stat_func_name]) for value_key in stat_value.keys())))
                rows.append([row_dict[key] for key in headers])
        # build DataTable specific structure with column headers
        dt_data = {
            "headers" : headers,
            "datatable_data" : rows,
            "datatable_columns" : [{"title" : header} for header in headers]
        }
        return dt_data

    def __get_stat_data(self, project, tablename, datestring, value_key, stat_func_name):
        """
        project <basestring>
        tablename <basestring>
        datestring <basestring>
        value_key <basestring>
        stat_func_name <basestring>

        extract statistical value for every index_key in this TimeseriesStatsArray for value_key

        return <list> of tuples (index_key, float(value))
        """
        self.__dl.setup(project, tablename, datestring)
        tsastats = self.__dl["tsastats"]
        stats_data = []
        for index_key, stats in tsastats.stats.items():
            stats_data.append((index_key, stats[value_key][stat_func_name]))
        return stats_data

    @staticmethod
    def __get_top_tuples(unsorted_list, number=10):
        """
        from some unsorted list with 2-tuples (<name>, <value of numeric type>)
        return top x elements and sum up all other elements to one newly generated item "other"

        returns <list> of <tuples>
        """
        # get top x-tuples
        data = sorted(unsorted_list, key=lambda a: a[1], reverse=True)[:number]
        # sum up al others
        data.append(("other", sum((value for name, value in sorted(unsorted_list, key=lambda a: a[1], reverse=True)[number:]))))
        # return sorted list of tuples
        return sorted(data, key=lambda a: a[1], reverse=True)

    @staticmethod
    def __get_hc_serie(data, name):
        """
        higchart specific dataset

        returns one serie
        """
        # generate some highcharts conform data
        hc_data = {
            "name" : name,
            "data" : []
        }
        for index_key, value in sorted(data, key=lambda a: a[1], reverse=True):
            hc_data["data"].append({
                "name": str(index_key),
                "y": value
                })
        return [hc_data,]

    def get_js_daily_top(self, *args, **kwds):
        """
        return total histogram of some
        project / tablename / datestring combination
        statistical function to use must alo be provided
        """
        project, tablename, datestring, value_keyname, stat_func_name = args
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
            self.__dl.setup(project, tablename, datestring) # datestring is not important
            assert value_keyname in self.__dl.value_keynames
            assert stat_func_name in self.__dl.stat_func_names
        except AssertionError:
            raise web.internalerror()
        # get raw data
        stats_data = self.__get_stat_data(project, tablename, datestring, value_keyname, stat_func_name)
        top_stats_data = self.__get_top_tuples(stats_data, 20)
        # highcharts specifics
        return self.__get_hc_serie(top_stats_data, value_keyname)

    @staticmethod
    def __get_hist_data(data, pct=True):
        """
        from some sort of input data like
        list of (<name>, <value numeric>)

        return normed / percent distribution like
        normed:
            ([0-100], absolut occurance for this percentil)
        optional percent:
            ([0-100], pct of total distribution (0-100) for this percentil)

        TODO: what about negative values?
        """
        # history dict
        hist = {}
        # get maximum value
        max_value = max((value for key, value in data))
        # print("max_value: ", max_value)
        # norm values 0-100
        if max_value != 0:
            normed = [int(100.0 * value / max_value) for key, value in data]
        else:
            normed = [0 for key, value in data]
        for counter in range(101):
            hist[counter] = 0
        # count appearance, histogram with absolute values
        for value in normed:
            hist[value] += 1
        if pct is True:
            # histogram with percentage values
            sum_hist_value = sum(hist.values())
            hist_pct = dict(((index, 100.0 * value / sum_hist_value) for index, value in hist.items()))
            return hist_pct.items()
        return hist.items()

    def get_js_total_histogram(self, *args, **kwds):
        """
        return total histogram of some
        project / tablename / datestring combination
        statistical function to use must alo be provided
        """
        project, tablename, datestring, value_keyname, stat_func_name = args
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
            self.__dl.setup(project, tablename, datestring) # datestring is not important
            assert value_keyname in self.__dl.value_keynames
            assert stat_func_name in self.__dl.stat_func_names
        except AssertionError:
            raise web.internalerror()
        # get raw data
        stats_data = self.__get_stat_data(project, tablename, datestring, value_keyname, stat_func_name)
        hist_data = self.__get_hist_data(stats_data, pct=True)
        return self.__get_hc_serie(hist_data, "percent")

    def get_js_total_longtime(self, *args, **kwds):
        project, tablename, datestring1, datestring2, value_keyname, stat_func_name = args
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
            self.__dl.setup(project, tablename, datestring1) # datestring is not important
            assert value_keyname in self.__dl.value_keynames
            assert stat_func_name in self.__dl.stat_func_names
        except AssertionError:
            raise web.internalerror()
        # get raw data
        hc_data = {
            "categories" : [],
            "value" : [],
            "total_count" : []
        }
        for datestring in self.__dl.datewalker(datestring1, datestring2):
            try:
                self.__dl.setup(project, tablename, datestring)
                stats_data = self.__dl["total_stats"]
                hc_data["categories"].append(datestring)
                hc_data["value"].append(stats_data[value_keyname][stat_func_name])
                hc_data["total_count"].append(stats_data[value_keyname]["total_count"])
            except KeyError:
                pass
        return hc_data

    def get_js_longtime(self, *args, **kwds):
        """
        return some longtime (between datestring an datestring2 statistical data

        parameters in this order:
        project <str>
        tablename <str>
        datestring1 <str> in form YYYY-MM-DD
        datestring2 <str> in form YYYY-MM-DD
        value_keyname <str>
        stat_func_name <str>
        index_key_b64 <str> base64 encoded index_key

        returns:
        HighCharts compatible dataset
        """
        project, tablename, datestring1, datestring2, value_keyname, stat_func_name, index_key_b64 = args
        index_key = b64eval(index_key_b64)
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
            self.__dl.setup(project, tablename, datestring1) # datestring is not important
            assert value_keyname in self.__dl.value_keynames
            assert stat_func_name in self.__dl.stat_func_names
        except AssertionError:
            raise web.internalerror()
        # get raw data
        # get raw data
        hc_data = {
            "categories" : [],
            "value" : [],
        }
        for datestring in self.__dl.datewalker(datestring1, datestring2):
            try:
                self.__dl.setup(project, tablename, datestring)
                stats_data = self.__dl["tsastats", index_key]
                hc_data["categories"].append(datestring)
                hc_data["value"].append(stats_data[value_keyname][stat_func_name])
            except KeyError:
                pass
        return hc_data

    def __get_ts_diff(self, project, tablename, datestring1, datestring2, index_key, group_func):
        """
        project <basestring>
        tablename <basestring>
        datestring1 <basestring> the older one
        datestring2 <basestring> the newer one
        index_key <basestring>
        group_func <func> to use for group_func(old-value, new_value)

        return <list> of dict structure, similar to timeseries from datalogger,
        but the timestamp is replaced with only the time portion of the second timeseries

        TODO: redesign this method, does not work properly
        """
        assert project in self.__dl.get_projects()
        assert tablename in self.__dl.get_tablenames(project)
        data = []
        self.__dl.setup(project, tablename, datestring1)
        ts1 = self.__dl["tsa", index_key]
        self.__dl.setup(project, tablename, datestring2)
        ts2 = self.__dl["tsa", index_key]
        # ts data is sorted by timestamp key, and should be the same length
        try:
            assert len(ts1) == len(ts2)
        except AssertionError as exc:
            print("skipping %s length of first timeseries %d, length of second timeseries %s" % (index_key, len(ts1), len(ts2)))
            # TODO: raise something more specific
            raise exc
        for index, item in enumerate(ts2):
            this_data = {}
            for key in item.colnames():
                if key in self.__dl.index_keynames:
                    this_data[key] = item[key]
                elif key == self.__dl.ts_keyname:
                    this_data["time"] = str(datetime.datetime.fromtimestamp(item["ts"]).time())
                else:
                    # TODO: be more specific about value_keyname to save
                    # memory ??
                    this_data[key] = group_func(ts1[index][key], ts2[index][key])
            data.append(this_data)
        return data

    def get_js_ts_diff(self, *args, **kwds):
        """
        return difference between one timeseries on two  different dates

        TODO: rework this
        """
        project, tablename, datestring1, datestring2, value_keyname, index_key_b64 = args
        index_key = b64eval(index_key_b64)
        group_func = lambda new, old: new - old
        # get raw data
        data = self.__get_ts_diff(project, tablename, datestring1, datestring2, index_key, group_func)
        # highcharts specific format
        # TODO: could this be the same as __get_hc_serie
        hc_data = {
            "value" : [item[value_keyname] for item in data],
            "categories" : [item["time"] for item in data],
        }
        return hc_data

    def __get_ts_filtered(self, project, tablename, datestring, index_pattern):
        """
        project <basestring>
        tablename <basestring>
        datestring <basestring> the older one
        index_pattern <basestring> - to filter index_names

        return dict of selected timeseries
        {
            index_key : TimeseriesData as provided by get_ts
        }
        """
        self.__dl.setup(project, tablename, datestring)
        ret_data = {}
        for index_key in self.__dl["tsa"].keys():
            index_key_dict = dict(zip(self.__dl.index_keynames, index_key))
            if all((index_pattern[key] in index_key_dict[key] for key in index_pattern.keys())):
                ret_data[index_key] = self.__dl["tsa", index_key]
        return ret_data

    def get_js_ts_stacked(self, *args, **kwds):
        """
        return Highcharts data for some - defined by pattern - timeseries
        every timseries is presented as own series in returned data
        """
        project, tablename, datestring, value_keyname = args[:4]
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
        except AssertionError as exc:
            raise web.internalerror("project or tablename does not exist")
        pattern_str = args[4:]
        pattern = dict(zip(pattern_str[::2], pattern_str[1::2]))
        self.__dl.setup(project, tablename, datestring)
        try:
            assert all((pattern_keyname in self.__dl.index_keynames for pattern_keyname in pattern.keys()))
        except AssertionError as exc:
            raise web.internalerror("some index_keynames not in table definition")
        # get raw data
        data = self.__get_ts_filtered(project, tablename, datestring, pattern)
        hc_data = []
        utc_offset = None
        for index_key, ts in data.items():
            if utc_offset is None:
                # get utc offset for this timestamp
                first_ts = ts[0, str(self.__dl.ts_keyname)] # TODO: remove str after python3 transition
                utc_offset = (datetime.datetime.fromtimestamp(first_ts) - datetime.datetime.utcfromtimestamp(first_ts)).total_seconds()
            hc_data.append({
                "name" : index_key,
                "data": [((ts[index, str("ts")] + utc_offset) * 1000, ts[index, str(value_keyname)]) for index, entry in enumerate(ts)] # TODO: remove  str after transition to python3
            })
        return hc_data

    def get_js_ts(self, *args, **kwds):
        """
        return single timeseries

        project <basestring>
        tablename <basestring>
        datestring <basestring> the older one
        index_key <basestring>

        return <list> of dict structure, similar to timeseries from datalogger,
        but the timestamp is replaced with only the time portion of the second timeseries
        """
        project, tablename, datestring, value_keyname, index_key_b64 = args[:5]
        index_key = b64eval(index_key_b64)
        assert project in self.__dl.get_projects()
        assert tablename in self.__dl.get_tablenames(project)
        self.__dl.setup(project, tablename, datestring)
        ts = self.__dl["tsa", index_key]
        # get utc offset for timestamp
        first_ts = ts[0, str(self.__dl.ts_keyname)] # TODO: in python2 str necessary, in python3 not necessary
        utc_offset = (datetime.datetime.fromtimestamp(first_ts) - datetime.datetime.utcfromtimestamp(first_ts)).total_seconds()
        hc_data = [] # must be list to work in highcharts
        hc_data.append({
            "name" : index_key,
            "data" : [((ts[index, str("ts")] + utc_offset) * 1000, ts[index, str(value_keyname)]) for index, entry in enumerate(ts)], # TODO: remove str in python3
        })
        return hc_data

    def __get_tsastat_diff(self, project, tablename, datestring1, datestring2, group_func, filter_value_keyname=None):
        """
        project <basestring>
        tablename <basestring>
        datestring1 <basestring> the older one
        datestring2 <basestring> the newer one
        index_key <basestring>
        group_func <func> to use for group_func(old-value, new_value)
        filter_value_keyname <str> to limit comparison to one single value_keyname

        return <list> of dict structure, similar to timeseries from datalogger,
        but the timestamp is replaced with only the time portion of the second timeseries
        """
        self.__dl.setup(project, tablename, datestring1)
        tsastats1 = self.__dl["tsastats"].stats
        self.__dl.setup(project, tablename, datestring2)
        tsastats2 = self.__dl["tsastats"].stats
        data = {}
        for index_key in tsastats1.keys():
            try:
                # every index_key has to be in both datasets to get
                # comparison
                assert index_key in tsastats2.keys()
            except AssertionError as exc:
                logging.info("skipping %s, not in both tsastats", index_key)
                continue
            data[index_key] = {}
            # ts data is sorted by timestamp key, and should be the same length
            for value_keyname in self.__dl.value_keynames:
                if filter_value_keyname is not None and filter_value_keyname != value_keyname:
                    continue
                data[index_key][value_keyname] = {}
                for stat_func_name in self.__dl.stat_func_names:
                    # newer value minus older value
                    data[index_key][value_keyname][stat_func_name] = group_func(tsastats2[index_key][value_keyname][stat_func_name], tsastats1[index_key][value_keyname][stat_func_name])
        return data

    def get_js_tsastats_diff(self, *args, **kwds):
        """
        get differential tsastat data from two dates
        """
        project, tablename, datestring1, datestring2, value_keyname = args
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
            self.__dl.setup(project, tablename, datestring1)
            assert value_keyname in self.__dl.value_keynames
            # check if datestring1 is the older one
            year, month, day = [int(token) for token in datestring1.split("-")]
            date1 = datetime.date(year, month, day)
            year, month, day = [int(token) for token in datestring2.split("-")]
            date2 = datetime.date(year, month, day)
            assert date2 > date1
        except AssertionError:
            raise web.internalerror("Argument error")
        # subtract older dataset from newer one
        group_func = lambda new, old: new - old
        # get differential data
        data = self.__get_tsastat_diff(project, tablename, datestring1, datestring2, group_func)
        # get only one specific value_keyname and row dictionaries
        # so this will be 2-dimensional
        rows = []
        headers = list(self.__dl.index_keynames) + ["value_keyname",] + list(self.__dl.stat_func_names)
        for index_key, stat_value in data.items():
            row_dict = dict(zip(self.__dl.index_keynames, index_key))
            row_dict["value_keyname"] = value_keyname
            row_dict.update(stat_value[value_keyname])
            rows.append([row_dict[key] for key in headers])
        # build DataTable specific structure with column headers
        dt_data = {
            "headers" : headers,
            "datatable_data" : rows,
            "datatable_columns" : [{"title" : header} for header in headers]
        }
        return dt_data

    def get_js_scatter(self, *args, **kwds):
        """
        return Highcharts Data for Scatter Plots
        """
        project, tablename, datestring, value_keyname1, value_keyname2, stat_func_name = args[:6]
        try:
            assert project in self.__dl.get_projects()
            assert tablename in self.__dl.get_tablenames(project)
            self.__dl.setup(project, tablename, datestring)
            assert value_keyname1 in self.__dl.value_keynames
            assert value_keyname2 in self.__dl.value_keynames
            assert stat_func_name in self.__dl.stat_func_names
        except AssertionError:
            raise web.internalerror("Argument error")
        scatter_data = []
        for index_key, stats in self.__dl["tsastats"].stats.items():
            scatter_data.append((index_key, (stats[value_keyname1][stat_func_name], stats[value_keyname2][stat_func_name])))
        hc_scatter_data = []
        for key, value_tuple in scatter_data:
            hc_scatter_data.append({
                "name" : str(key),
                "data" : (value_tuple, )
            })
        return hc_scatter_data

    def get_js_vicenter_cpu_unused(self, *args):
        """
        special report to find virtual machine which re not used their virtual core entirely
        on this machine there is a possibility to save some virtual cores

        works only for VMware machines, in special virtualMachineCpuStats6
        """
        self.__dl.setup("vicenter", "virtualMachineCpuStats6", args[0])
        # build grouped data
        tsastat_g = {}
        for index_key, tsstat in self.__dl["tsastats"].stats.items():
            if index_key[0] not in tsastat_g:
                tsastat_g[(index_key[0], )] = tsstat
            else:
                for value_keyname in tsstat.keys():
                    for stat_func_name in tsstat[value_keyname].keys():
                        tsastat_g[(index_key[0], )][value_keyname][stat_func_name] += tsstat[value_keyname][stat_func_name]
        data = []
        headers = ("hostname", "avg_idle_min", "avg_used_avg", "avg_used_max")
        for index_key, tsstat in tsastat_g.items():
            data.append({
                "hostname": index_key[0],
                "avg_idle_min" : "%0.2f" % tsstat["cpu.idle.summation"]["min"],
                "avg_used_avg" : "%0.2f" % tsstat["cpu.used.summation"]["avg"],
                "avg_used_max" : "%0.2f" % tsstat["cpu.used.summation"]["max"]
            })
        return self.__to_dt_format(data, headers)

    def get_js_hrstorage_mem_unused(self, *args, **kwds):
        """
        special report to find servers which are not using their ram entirely
        specially on virtual machines this is a huge saving potential

        works only for snmp/hrStorageTable
        """
        self.__dl.setup("snmp", "hrStorageTable", args[0])
        headers = ("hostname", "hrStorageSizeKb", "hrStorageUsedKb", "hrStorageNotUsedKbMin", "hrStorageNotUsedPct")
        data = []
        for index_key, tsstat in self.__dl["tsastats"].stats.items():
            if u'HOST-RESOURCES-TYPES::hrStorageRam' not in index_key:
                continue
            sizekb = tsstat["hrStorageSize"]["min"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            usedkb = tsstat["hrStorageUsed"]["max"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            notused = sizekb - usedkb
            notused_pct = 100.0 *  notused / sizekb
            data.append({
                "hostname" : index_key[0],
                "hrStorageSizeKb" : "%0.2f" % sizekb,
                "hrStorageUsedKb" : "%0.2f" % usedkb,
                "hrStorageNotUsedKbMin" : "%0.2f" % notused,
                "hrStorageNotUsedPct" : "%0.2f" % notused_pct
            })
        return self.__to_dt_format(data, headers)

    def get_js_hrstorage_disk_unused(self, *args, **kwds):
        """
        special report to get a report of unused SNMP Host Storage
        works only with snmp/hrStorageTable
        """
        self.__dl.setup("snmp", "hrStorageTable", args[0])
        storage_type = "hrStorageFixedDisk"
        data = []
        headers = ("hostname", "hrStorageDescr", "hrStorageSizeKb", "hrStorageUsedKb", "hrStorageNotUsedKbMin", "hrStorageNotUsedPct")
        for index_key, tsstat in self.__dl["tsastats"].stats.items():
            if (u"HOST-RESOURCES-TYPES::%s" % storage_type) not in index_key:
                continue
            if index_key[1][:4] in (u"/run", u"/dev", u"/sys"):
                continue
            sizekb = tsstat["hrStorageSize"]["min"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            usedkb = tsstat["hrStorageUsed"]["max"] * tsstat["hrStorageAllocationUnits"]["max"] / 1024
            notused = sizekb - usedkb
            notused_pct = 0.0
            try:
                notused_pct = 100.0 *  notused / sizekb
            except ZeroDivisionError:
                pass
            data.append({
                "hostname" : index_key[0],
                "hrStorageDescr" : index_key[1],
                "hrStorageSizeKb" : "%0.2f" % sizekb,
                "hrStorageUsedKb" : "%0.2f" % usedkb,
                "hrStorageNotUsedKbMin" : "%0.2f" % notused,
                "hrStorageNotUsedPct" : "%0.2f" % notused_pct
            })
        return self.__to_dt_format(data, headers)

    @staticmethod
    def __to_dt_format(row_data, headers):
        """
        return datatable formatted structure to use in JS Application

        row_data <list> of <dict> data for every row
        headers <tuple> for column headers, must be key in row_data

        returns:
        {
            headers : <list>  of headers
            datatable_data : <list> of <dict> every row
            datatable_columns : <list> of <dict> mapping between headers and datatable_data
        }
        """
        # build DataTable specific structure with column headers
        assert isinstance(headers, tuple)
        assert isinstance(row_data, list)
        if len(row_data) > 0:
            assert isinstance(row_data[0], dict)
            assert len(row_data[0].keys()) == len(headers)
            assert all((header in row_data[0].keys() for header in headers))
        rows = []
        for row_dict in row_data:
            rows.append([row_dict[key] for key in headers])
        dt_data = {
            "headers" : headers,
            "datatable_data" : rows,
            "datatable_columns" : [{"title" : header} for header in headers]
        }
        return dt_data


################################### POST Section ###############################################

    @authenticator
    @jsonout
    def POST(self, parameters):
        """
        GET Multiplexer function, according to first argument in URL
        call this function, and resturn result to client

        parameters:
        /<str>function_name/...

        return:
        return function_name(what is left of arguments)

        """
        self.logger.info("POST calling %s", parameters)
        args = parameters.strip("/").split("/")
        # build method name from url
        method = "post_%s" % args[0].lower()
        query = dict(web.input()) # get query as dict
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
        except AssertionError as exc:
            self.logger.exception(exc)
            os.unlink(filename)
            self.logger.info("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
            return "Invalid data in uploaded file, see apache error log for details, uploaded file not stored"
        self.logger.info("File stored")
        return "File stored"

################################### PUT Section ###############################################

    @authenticator
    @jsonout
    def PUT(self, parameters):
        """
        generelle HTTP PUT Method is used to add individual data to some files.

        data can only be added to files of today
        this method is meant to add live data several times a day

        data must be in json format as list of individual dict
        dict must be in defines format of project/tablename

        parameters:
        /project/tablename
        data = json formatted list of dicts
        """
        self.logger.info("PUT calling %s", parameters)
        args = parameters.strip("/").split("/")
        # build method name from url
        project, tablename = args[:2]
        datestring = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
        self.__dl.setup(project, tablename, datestring)
        ts = time.time()
        filename = os.path.join(self.__dl.raw_basedir, "%s_%s.csv" % (tablename, datestring))
        data = dict(web.input()) # get query as dict, use input not data
        # self.logger.info("received data to store %s", data)
        if not all((index_key in data for index_key in self.__dl.index_keynames)):
            self.logger.error("some index_key is missing")
            return
        if not all((value_key in data for value_key in self.__dl.value_keynames)):
            self.logger.error("some value_key is missing")
            return
        if not self.__dl.ts_keyname in data:
            self.logger.error("ts_key is missing")
            return
        # check timestamp
        min_ts = ts - 60
        max_ts = ts + 60
        if not (min_ts < float(data[self.__dl.ts_keyname]) < max_ts):
            self.logger.info("timestamp in received data is out of range +/- 60s")
            return
        # actualy append or write
        if os.path.isfile(filename):
            self.logger.info("raw file does already exist, appending")
            with open(filename, "at") as outfile:
                outfile.write("\t".join([data[key] for key in self.__dl.headers]))
                outfile.write("\n")
        else:
            self.logger.info("raw file does not exst, will create new file %s", filename)
            with open(filename, "wt") as outfile:
                outfile.write("\t".join(self.__dl.headers))
                outfile.write("\n")
                outfile.write("\t".join([data[key] for key in self.__dl.headers]))
                outfile.write("\n")

################################### PUT Section ###############################################

    @authenticator
    @jsonout
    def DELETE(self, parameters):
        """
        GET Multiplexer function, according to first argument in URL
        call this function, and resturn result to client

        parameters:
        /<str>function_name/...

        return:
        return function_name(what is left of arguments)

        """
        self.logger.info("DELETE calling %s", parameters)
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

if __name__ == "__main__":
    # TESTING only, starts cherrypy webserver
    app = web.application(urls, globals())
    app.run() # will start local webserver
else:
    app = web.application(urls, globals())
    application = app.wsgifunc() # this must be named application
