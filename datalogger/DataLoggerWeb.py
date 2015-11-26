#!/usr/bin/python

import urllib
import urllib2
from collections import OrderedDict as OrderedDict
import json
import base64
import unittest
import logging
# own modules
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import QuantillesArray as QuantillesArray

DATALOGGER_URL = "http://srvmgdata1.tilak.cc/DataLogger"

class DataLoggerWeb(object):
    """
    class wot work with DataLogger Web Application
    """

    def __init__(self, datalogger_url):
        """
        parameters:
        datalogger_url <str> baseURL to use for every call
        """
        self.__datalogger_url = datalogger_url

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

#    def __urlencode_json(self, data):
#        return urllib.quote_plus(json.dumps(data).replace(" ", ""))

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

    def get_quantilles(self, project, tablename, datestring):
        """
        get QuantillesArray object for this particular project/tablename/datestring combination

        parameters:
        project <str>
        tablename <str>
        datestring <str>

        returns:
        <QuantillesArray>
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
            "datestring" : datestring,
        }
        query_params = {}
        data = self.__get_raw_data("get_quantilles", uri_params, query_params)
        quantilles = QuantillesArray.from_json(data)
        return quantilles


class Test(unittest.TestCase):

    datalogger = DataLoggerWeb(DATALOGGER_URL)

    def test_get_projects(self):
        data = self.datalogger.get_projects()
        self.assertTrue(isinstance(data, list))

    def test_get_tablenames(self):
        data = self.datalogger.get_tablenames("vicenter")
        self.assertTrue(isinstance(data, list))

    def test_get_wikiname(self):
        data = self.datalogger.get_wikiname("ucs", "ifTable")
        self.assertTrue(isinstance(data, basestring))

    def test_get_headers(self):
        data = self.datalogger.get_headers("ucs", "ifTable")
        print data

    def test_get_index_keynames(self):
        data = self.datalogger.get_index_keynames("ucs", "ifTable")
        print data

    def test_get_value_keynames(self):
        data = self.datalogger.get_value_keynames("ucs", "ifTable")
        print data

    def test_get_ts_keyname(self):
        data = self.datalogger.get_ts_keyname("ucs", "ifTable")
        print data

    def test_get_last_business_day_datestring(self):
        data = self.datalogger.get_last_business_day_datestring()
        print data

    def test_get_datewalk(self):
        data = self.datalogger.get_datewalk("2015-11-01", self.datalogger.get_last_business_day_datestring())
        print data
        self.assertGreater(len(data), 1)

    def test_get_caches(self):
        caches = self.datalogger.get_caches("ucs", "ifTable", "2015-11-06")
        #print caches
        print type(caches)
        print caches.keys()
        for key, filename in caches["ts"]["keys"].items()[:10]:
            print "getting ts for key %s" % key
            self.datalogger.get_ts("ucs", "ifTable", "2015-11-06", key)

    def test_get_tsa(self):
        tsa = self.datalogger.get_tsa("ucs", "ifTable", "2015-11-06")
        print tsa
        print type(tsa)
        print tsa.keys()

    def test_get_ts(self):
        tsa = self.datalogger.get_ts("ucs", "ifTable", "2015-11-06", (u'ucsfia-sr2-1-mgmt0', u'Vethernet9175', u'gigabitEthernet'))
        print tsa
        print type(tsa)
        print tsa.keys()
        print len(tsa.keys())

    def test_get_tsastats(self):
        tsastats = self.datalogger.get_tsastats("ucs", "ifTable", "2015-11-06")
        print tsastats
        tsstat = tsastats[tsastats.keys()[0]]
        print tsstat
        print type(tsstat)
        print tsstat["ifSpeed"]["avg"]
        print type(tsstat["ifSpeed"]["avg"])

    def test_get_quantilles(self):
        qa = self.datalogger.get_quantilles("ucs", "ifTable", "2015-11-06")
        print qa
        print type(qa)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
