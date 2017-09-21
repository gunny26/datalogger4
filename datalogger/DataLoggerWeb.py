#!/usr/bin/python

import urllib
import urllib2
from collections import OrderedDict as OrderedDict
import json
import base64
import logging
import os
# own modules
from datalogger import Timeseries as Timeseries
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import TimeseriesStats as TimeseriesStats
from datalogger import QuantileArray as QuantileArray


class DataLoggerWeb(object):
    """
    class wot work with DataLogger Web Application
    """

    def __init__(self, datalogger_url=None):
        """
        parameters:
        datalogger_url <str> baseURL to use for every call
        """
        if datalogger_url is not None:
            self.__datalogger_url = datalogger_url
        else:
            logging.debug("reading datalogger_url from config file")
            conffile = "/etc/datalogger/datalogger.conf"
            if os.path.isfile(conffile):
                for row in open(conffile, "rb").read().split("\n"):
                    if len(row) > 0 and row[0] != "#":
                        key, value = row.split("=")
                        self.__dict__[key.strip()] = value.strip()
                        logging.debug("%s = %s", key.strip(), self.__dict__[key.strip()])
                        self.__datalogger_url = self.__dict__[key.strip()]
        logging.info(self.__dict__)
        assert self.__datalogger_url is not None

    def __get_url(self, method, uri_params, query_params):
        """
        creates url to call with urllib

        parameters:
        uri_parameters <dict> are appended in URL
        query_parameters <dict> are appended after ? in GET Requests

        returns:
        <str> URL to call with urllib
        """
        url = "/".join((self.__datalogger_url, method))
        url = "/".join((url, "/".join((value for key, value in uri_params.items()))))
        if len(query_params.keys()) > 0:
            url = "?".join((url, "&".join(("%s=%s" % (key, value) for key, value in query_params.items()))))
        logging.debug("created url: %s", url)
        return url

    def __get_json(self, method, uri_params, query_params):
        """
        call url and parse the returned data with json.loads

        parameters:
        method <str> Web Application Function to call
        uri_parameters <dict>
        query_parameters <dict>

        returns:
        <object> returnd from json.loads(returned data from urllib)
        """
        raw = self.__get_raw_data(method, uri_params, query_params)
        try:
            ret = json.loads(raw)
            #logging.debug("JSON Output:\n%s", ret)
            return ret
        except ValueError as exc:
            logging.exception(exc)
            logging.error("JSON decode error, raw output: %s", raw)

    def __get_raw_data(self, method, uri_params, query_params):
        """
        call url return data received

        parameters:
        method <str> Web Application Function to call
        uri_parameters <dict>
        query_parameters <dict>

        returns:
        <str> returnd from urllib
        """
        url = self.__get_url(method, uri_params, query_params)
        try:
            res = urllib2.urlopen(url)
            logging.debug("got Status code : %s", res.code)
            raw = res.read()
        #logging.debug("Raw Output: %s", raw)
            return raw
        except Exception as exc:
            logging.exception(exc)
            logging.error("Error occured calling %s", url)
            raise exc

    def __get_json_chunked(self, method, uri_params, query_params):
        """
        call url return data received

        parameters:
        method <str> Web Application Function to call
        uri_parameters <dict>
        query_parameters <dict>

        returns:
        <str> returnd from urllib
        """
        url = self.__get_url(method, uri_params, query_params)
        try:
            res = urllib2.urlopen(url)
            logging.debug("got Status code : %s", res.code)
            data = ""
            raw = res.read()
            while raw:
                try:
                    data += raw
                    raw = res.read()
                except ValueError as exc:
                    logging.exception(exc)
            return json.loads(data)
        except Exception as exc:
            logging.exception(exc)
            logging.error("Error occured calling %s", url)
            raise exc

    def get_projects(self):
        """
        get list of available projects

        returns:
        <list>
        """
        uri_params = {}
        query_params = {}
        data = self.__get_json("get_projects", uri_params, query_params)
        return data

    def get_tablenames(self, project):
        """
        get list of tablenames of this project

        parameters:
        project <str>

        returns:
        <list>
        """
        uri_params = {
            "project" : project,
        }
        query_params = {}
        data = self.__get_json("get_tablenames", uri_params, query_params)
        return data

    def get_wikiname(self, project, tablename):
        """
        get wikiname for given project tablename

        parameters:
        project <str>
        tablename <str>

        returns:
        <str>
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename
        }
        query_params = {}
        data = self.__get_json("get_wikiname", uri_params, query_params)
        return data

    def get_headers(self, project, tablename):
        """
        get headers of raw data of this particular project/tablename combination

        parameters:
        project <str>
        tablename <str>

        returns:
        <list> of headers
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_headers", uri_params, query_params)
        return data

    def get_index_keynames(self, project, tablename):
        """
        get index_keynames of this particular project/tablename combination

        parameters:
        project <str>
        tablename <str>

        returns:
        <list> of index_keynames
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_index_keynames", uri_params, query_params)
        return data

    def get_value_keynames(self, project, tablename):
        """
        get value_keynames of this particular project/tablename combination

        parameters:
        project <str>
        tablename <str>

        returns:
        <list> of value_keynames
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_value_keynames", uri_params, query_params)
        return data

    def get_ts_keyname(self, project, tablename):
        """
        get ts_keyname of this particular project/tablename combination

        parameters:
        project <str>
        tablename <str>

        returns:
        <str> ts_keyname used
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_ts_keyname", uri_params, query_params)
        return data

    def get_last_business_day_datestring(self):
        """
        get last business day datestring from WebApplication

        returns:
        <str> datestring like 2015-12-31
        """
        uri_params = {}
        query_params = {}
        data = self.__get_json("get_last_business_day_datestring", uri_params, query_params)
        return data

    def get_datewalk(self, datestring1, datestring2):
        """
        get last business day datestring from WebApplication

        returns:
        <str> datestring like 2015-12-31
        """
        uri_params = OrderedDict()
        uri_params["datestring1"] = datestring1
        uri_params["datestring2"] = datestring2
        query_params = {}
        data = self.__get_json("get_datewalk", uri_params, query_params)
        return data


    def get_caches(self, project, tablename, datestring):
        """
        get ts_keyname of this particular project/tablename combination

        parameters:
        project <str>
        tablename <str>
        datestring <str>

        returns:
        <dict> of caches available
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
            "datestring" : datestring
        }
        query_params = {}
        data = self.__get_json("get_caches", uri_params, query_params)
        return data

    def get_tsa(self, project, tablename, datestring):
        """
        get TimeseriesArray object for this particular project/tablename/datestring combination

        parameters:
        project <str>
        tablename <str>
        datestring <str>

        returns:
        <TimeseriesArray>
        """
        index_keynames = self.get_index_keynames(project, tablename)
        value_keynames = self.get_value_keynames(project, tablename)
        ts_keyname = self.get_ts_keyname(project, tablename)
        tsa = TimeseriesArray(index_keynames, value_keynames, ts_keyname)
        uri_params = {
            "project" : project,
            "tablename" : tablename,
            "datestring" : datestring,
        }
        query_params = {}
        data = self.__get_json_chunked("get_tsa", uri_params, query_params)
        for row in data:
            tsa.add(row)
        return tsa

    def get_tsa_adv(self, project, tablename, datestring, groupkeys, group_func_name, index_pattern):
        """
        get TimeseriesArray object for this particular project/tablename/datestring combination

        parameters:
        project <str>
        tablename <str>
        datestring <str>
        groupkeys <tuple>
        group_func_name <str>
        index_pattern <str>

        returns:
        <TimeseriesArray>
        """
        value_keynames = self.get_value_keynames(project, tablename)
        ts_keyname = self.get_ts_keyname(project, tablename)
        tsa = None
        if groupkeys is None:
            index_keynames = self.get_index_keynames(project, tablename)
            tsa = TimeseriesArray(index_keynames, value_keynames, ts_keyname)
        else:
            tsa = TimeseriesArray(groupkeys, value_keynames, ts_keyname)
        uri_params = OrderedDict()
        uri_params["project"] = project
        uri_params["tablename"] = tablename
        uri_params["datestring"] = datestring
        uri_params["groupkey_enc"] = base64.b64encode(unicode(groupkeys))
        uri_params["group_func_name"] = group_func_name
        uri_params["index_pattern"] = base64.b64encode(unicode(index_pattern))
        query_params = {}
        data = self.__get_json_chunked("get_tsa_adv", uri_params, query_params)
        for row in data:
            tsa.add(row)
        return tsa

    def get_ts(self, project, tablename, datestring, key):
        """
        get Timeseries object for this particular project/tablename/datestring/key combination

        parameters:
        project <str>
        tablename <str>
        datestring <str>
        key <tuple> key to identify particular Timeseries

        returns:
        <TimeseriesArray>
        """
        index_keynames = self.get_index_keynames(project, tablename)
        value_keynames = self.get_value_keynames(project, tablename)
        ts_keyname = self.get_ts_keyname(project, tablename)
        tsa = TimeseriesArray(index_keynames, value_keynames, ts_keyname)
        uri_params = {
            "project" : project,
            "tablename" : tablename,
            "datestring" : datestring,
            "key" : base64.b64encode(unicode(key)),
        }
        query_params = {}
        data = self.__get_json_chunked("get_ts", uri_params, query_params)
        for row in data:
            tsa.add(row)
        return tsa

    def get_tsastats(self, project, tablename, datestring):
        """
        get TimeseriesStatsArray object for this particular project/tablename/datestring combination

        parameters:
        project <str>
        tablename <str>
        datestring <str>

        returns:
        <TimeseriesStatsArray>
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
            "datestring" : datestring,
        }
        query_params = {}
        data = self.__get_raw_data("get_tsastats", uri_params, query_params)
        tsastats = TimeseriesArrayStats.from_json(data)
        return tsastats

    def get_stat_func_names(self):
        """
        get statistical functions defined in TimeseriesArrayStats

        returns:
        <list>
        """
        uri_params = {}
        query_params = {}
        data = self.__get_json("get_stat_func_names", uri_params, query_params)
        return data

    def get_quantile(self, project, tablename, datestring):
        """
        get QuantileArray object for this particular project/tablename/datestring combination

        parameters:
        project <str>
        tablename <str>
        datestring <str>

        returns:
        <QuantileArray>
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
            "datestring" : datestring,
        }
        query_params = {}
        data = self.__get_raw_data("get_quantile", uri_params, query_params)
        quantile = QuantileArray.from_json(data)
        return quantile
