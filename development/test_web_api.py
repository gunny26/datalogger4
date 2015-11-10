#!/usr/bin/python

import urllib
import urllib2
from collections import OrderedDict as OrderedDict
import json
import unittest
import logging
logging.basicConfig(level=logging.DEBUG)
from datalogger import DataLoggerWeb as DataLoggerWeb
#from datalogger import TimeseriesArray as TimeseriesArray
#from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
#from datalogger import QuantillesArray as QuantillesArray

DATALOGGER_URL = "http://srvmgdata1.tilak.cc/DataLogger"

class DataLoggerWeb_local(object):

    def __init__(self, datalogger_url):
        self.__datalogger_url = datalogger_url
        pass

    def __get_url(self, method, uri_params, query_params):
        url = "/".join((self.__datalogger_url, method))
        url = "/".join((url, "/".join((value for key, value in uri_params.items()))))
        if len(query_params.keys()) > 0:
            url = "?".join((url, "&".join(("%s=%s" % (key, value) for key, value in query_params.items()))))
        logging.info("url: %s", url)
        return(url)

    def __get_json(self, method, uri_params, query_params):
        try:
            ret = json.loads(self.__get_raw_data(method, uri_params, query_params))
            #logging.debug("JSON Output:\n%s", ret)
            return(ret)
        except ValueError as exc:
            logging.exception(exc)
            logging.error("JSON decode error, raw output: %s", raw)

    def __get_raw_data(self, method, uri_params, query_params):
        res = urllib2.urlopen(self.__get_url(method, uri_params, query_params))
        print res.code
        raw = res.read()
        #logging.debug("Raw Output: %s", raw)
        return raw

    def __urlencode_json(self, data):
        return(urllib.quote_plus(json.dumps(data).replace(" ", "")))

    def get_projects(self):
        uri_params = {}
        query_params = {}
        data = self.__get_json("get_projects", uri_params, query_params)
        return data

    def get_tablenames(self, project):
        uri_params = {
            "project" : project,
        }
        query_params = {}
        data = self.__get_json("get_tablenames", uri_params, query_params)
        return data

    def get_headers(self, project, tablename):
        """
        get headers of raw data of this particular project/tablename combination
        """
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_headers", uri_params, query_params)
        return data

    def get_index_keynames(self, project, tablename):
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_index_keynames", uri_params, query_params)
        return data

    def get_value_keynames(self, project, tablename):
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_value_keynames", uri_params, query_params)
        return data

    def get_ts_keyname(self, project, tablename):
        uri_params = {
            "project" : project,
            "tablename" : tablename,
        }
        query_params = {}
        data = self.__get_json("get_ts_keyname", uri_params, query_params)
        return data

    def get_last_business_day_datestring(self):
        uri_params = {}
        query_params = {}
        data = self.__get_json("get_last_business_day_datestring", uri_params, query_params)
        return data

    def get_tsa(self, project, tablename, datestring):
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
        data = self.__get_json("get_tsa", uri_params, query_params)
        for row in data:
            tsa.add(row)
        return tsa

    def get_tsastats(self, project, tablename, datestring):
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

    def test_get_tsa(self):
        tsa = self.datalogger.get_tsa("ucs", "ifTable", "2015-11-06")
        print tsa

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
    unittest.main()
