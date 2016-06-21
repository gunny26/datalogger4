#!/usr/bin/python

import urllib
import urllib2
from collections import OrderedDict as OrderedDict
import json
import base64
import unittest
import logging
import os
# own modules
from datalogger import Timeseries as Timeseries
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import TimeseriesStats as TimeseriesStats
from datalogger import QuantileArray as QuantileArray

DATALOGGER_URL = "http://srvmgdata1.tilak.cc/DataLogger"

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
            logging.info("reading datalogger_url from config file")
            conffile = "/etc/datalogger/datalogger.conf"
            if os.path.isfile(conffile):
                for row in open(conffile, "rb").read().split("\n"):
                    if len(row) > 0 and row[0] != "#":
                        key, value = row.split("=")
                        self.__dict__[key.strip()] = value.strip()
                        logging.info("%s = %s", key.strip(), self.__dict__[key.strip()])
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
        except StandardError as exc:
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
        except StandardError as exc:
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


class Test(unittest.TestCase):

    datalogger = DataLoggerWeb()

    def test_get_projects(self):
        data = self.datalogger.get_projects()
        self.assertTrue(isinstance(data, list))
        assert u"ucs" in data

    def test_get_tablenames(self):
        data = self.datalogger.get_tablenames("ucs")
        self.assertTrue(isinstance(data, list))
        assert "ifTable" in data

    def test_get_wikiname(self):
        data = self.datalogger.get_wikiname("ucs", "ifTable")
        self.assertTrue(isinstance(data, basestring))
        assert data == u"DataLoggerReportUcsIftable"

    def test_get_headers(self):
        data = self.datalogger.get_headers("ucs", "ifTable")
        assert isinstance(data, list)
        assert data == [u'hostname', u'ifAdminStatus', u'ifDescr', u'ifInDiscards', u'ifInErrors',
            u'ifInNUcastPkts', u'ifInOctets', u'ifInUcastPkts', u'ifInUnknownProtos', u'ifIndex',
            u'ifLastChange', u'ifMtu', u'ifOperStatus', u'ifOutDiscards', u'ifOutErrors', u'ifOutNUcastPkts',
            u'ifOutOctets', u'ifOutQLen', u'ifOutUcastPkts', u'ifPhysAddress', u'ifSpecific',
            u'ifSpeed', u'ifType', u'index', u'ts']

    def test_get_index_keynames(self):
        data = self.datalogger.get_index_keynames("ucs", "ifTable")
        assert isinstance(data, list)
        assert data == [u'hostname', u'ifDescr', u'ifType']

    def test_get_value_keynames(self):
        data = self.datalogger.get_value_keynames("ucs", "ifTable")
        assert isinstance(data, list)
        assert data == [u'ifInDiscards', u'ifInErrors', u'ifInOctets', u'ifSpeed', u'ifOutQLen',
            u'ifInUcastPkts', u'ifOutNUcastPkts', u'ifOutDiscards', u'ifOutOctets', u'ifOutErrors',
            u'ifInUnknownProtos', u'ifOutUcastPkts', u'ifInNUcastPkts', u'ifMtu']

    def test_get_ts_keyname(self):
        data = self.datalogger.get_ts_keyname("ucs", "ifTable")
        assert isinstance(data, basestring)
        assert data == u"ts"

    def test_get_last_business_day_datestring(self):
        data = self.datalogger.get_last_business_day_datestring()
        assert isinstance(data, basestring)

    def test_get_datewalk(self):
        data = self.datalogger.get_datewalk("2015-11-01", "2015-11-30")
        assert u"2015-11-26" in data
        assert u"2015-10-31" not in data
        assert u"2015-12-01" not in data
        self.assertGreater(len(data), 1)

    def test_get_caches(self):
        caches = self.datalogger.get_caches("ucs", "ifTable", "2015-11-06")
        assert isinstance(caches, dict)
        assert caches.keys() == [u'tsa', u'tsstat', u'tsastat', u'ts']
        for key, filename in caches["ts"]["keys"].items()[:10]:
            tsa = self.datalogger.get_ts("ucs", "ifTable", "2015-11-06", key)
            assert isinstance(tsa, TimeseriesArray)
            ts = tsa[tsa.keys()[0]]
            assert isinstance(ts, Timeseries)
            assert len(ts) > 0

    def test_get_tsa(self):
        tsa = self.datalogger.get_tsa("ucs", "ifTable", "2015-11-06")
        assert isinstance(tsa, TimeseriesArray)
        assert len(tsa.keys()) == 570

    def test_get_tsa_adv(self):
        tsa = self.datalogger.get_tsa_adv("ucs", "ifTable", "2015-11-06", None, "avg", "(.*)Ethernet(.*)")
        assert isinstance(tsa, TimeseriesArray)
        assert all((len(key) == 3 for key in tsa.keys()))
        assert all((u"Ethernet" in unicode(key) for key in tsa.keys()))
        tsa = self.datalogger.get_tsa_adv("ucs", "ifTable", "2015-11-06", ("hostname", ), "max", "(.*)gigabit(.*)")
        assert isinstance(tsa, TimeseriesArray)
        assert all((len(key) == 1 for key in tsa.keys()))
        tsa = self.datalogger.get_tsa_adv("ucs", "ifTable", "2015-11-06", (), "sum", "(.*)port-channel(.*)")
        assert isinstance(tsa, TimeseriesArray)
        assert len(tsa.keys()) == 1
        assert tsa.keys()[0] == ()

    def test_get_ts(self):
        tsa = self.datalogger.get_ts("ucs", "ifTable", "2015-11-06", (u'ucsfia-sr2-1-mgmt0', u'Vethernet9175', u'gigabitEthernet'))
        assert isinstance(tsa, TimeseriesArray)
        assert len(tsa.keys()) == 1
        assert tsa.keys()[0] == (u'ucsfia-sr2-1-mgmt0', u'Vethernet9175', u'gigabitEthernet')

    def test_get_tsastats(self):
        tsastats = self.datalogger.get_tsastats("ucs", "ifTable", "2015-11-06")
        assert isinstance(tsastats, TimeseriesArrayStats)
        assert len(tsastats.keys()) > 1
        tsstat = tsastats[tsastats.keys()[0]]
        assert isinstance(tsstat, TimeseriesStats)
        tsstat["ifSpeed"]["avg"]
        assert type(tsstat["ifSpeed"]["avg"]) == float

    def test_get_stat_func_names(self):
        data = self.datalogger.get_stat_func_names()
        assert isinstance(data, list)
        assert len(data) > 1
        assert data == [u'std', u'count', u'last', u'min', u'mean', u'max', u'sum', u'avg', u'median', u'first']

    def test_get_quantile(self):
        qa = self.datalogger.get_quantile("ucs", "ifTable", "2015-11-06")
        assert isinstance(qa, QuantileArray)
        # TODO: do more checking

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
