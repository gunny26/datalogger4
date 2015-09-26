#!/usr/bin/python

import urllib
import urllib2
from collections import OrderedDict as OrderedDict
import json
import unittest
import logging
logging.basicConfig(level=logging.INFO)

DATALOGGER_URL = "http://srvmghomer.tilak.cc/datalogger/RawData"

def test_get_vicenter_timeseries():
    params = {}
    params["project"] = "vicenter"
    params["tablename"] = "virtualMachineDatastoreStats"
    params["servername"] = "srvpralki1.tilak.cc"
    params["instance"] = "5305cfcc-8b500331-071a-0025b511004f"
    params["datestring"] = "2015-05-23"
    url = "http://srvmghomer.tilak.cc/datalogger/RawData/getVicenterTimeseries/%(tablename)s/%(datestring)s/%(servername)s/%(instance)s" % params
    logging.info("calling %s", url)
    res = urllib2.urlopen(url)
    logging.info("got status %s", res.code)
    logging.info("Output:\n%s", res.read())

class Test(unittest.TestCase):

    def __get_url(self, method, uri_params, query_params):
        url = "/".join((DATALOGGER_URL, method))
        url = "/".join((url, "/".join((value for key, value in uri_params.items()))))
        if len(query_params.keys()) > 0:
            url = "?".join((url, "&".join(("%s=%s" % (key, value) for key, value in query_params.items()))))
        logging.info("url: %s", url)
        return(url)

    def __get_json(self, method, uri_params, query_params):
        res = urllib2.urlopen(self.__get_url(method, uri_params, query_params))
        self.assertTrue(res.code < 400)
        raw = res.read()
        logging.debug("Raw Output: %s", raw)
        try:
            ret = json.loads(raw)
            logging.debug("JSON Output:\n%s", ret)
            return(ret)
        except ValueError as exc:
            logging.exception(exc)
            logging.error("JSON decode error, raw output: %s", raw)

    def __urlencode_json(self, data):
        return(urllib.quote_plus(json.dumps(data).replace(" ", "")))

    def test_get_headers(self):
        uri_params = {
            "project" : "test",
            "tablename" : "fcIfC3AccountingTable"
        }
        query_params = {}
        data = self.__get_json("get_headers", uri_params, query_params)
        self.assertTrue(isinstance(data, list))

    def test_get_index_keynames(self):
        uri_params = {
            "project" : "test",
            "tablename" : "fcIfC3AccountingTable"
        }
        query_params = {}
        data = self.__get_json("get_index_keynames", uri_params, query_params)
        self.assertTrue(isinstance(data, list))

    def test_get_value_keynames(self):
        uri_params = {
            "project" : "test",
            "tablename" : "fcIfC3AccountingTable"
        }
        query_params = {}
        data = self.__get_json("get_value_keynames", uri_params, query_params)
        self.assertTrue(isinstance(data, list))

    def test_get_ts_keyname(self):
        uri_params = {
            "project" : "test",
            "tablename" : "fcIfC3AccountingTable"
        }
        query_params = {}
        data = self.__get_json("get_ts_keyname", uri_params, query_params)
        self.assertTrue(isinstance(data, basestring))

    def test_get_chart_data_ungrouped(self):
        uri_params = OrderedDict()
        uri_params[u"project"] = u'ipstor'
        uri_params[u"tablename"] = u'vrsClientTable'
        uri_params[u"datestring"] = u'2015-06-10'
        uri_params[u"keys"] =  self.__urlencode_json((u'vsanapp3', u'VMWSR131', u'VMWARE_PROD-3_SAS_SFF_L008', ))
        uri_params[u"value_keys"] = self.__urlencode_json((u'vrsclSCSIReadCmd', ))
        uri_params[u"datatype"] = self.__urlencode_json("absolute")
        uri_params[u"group"] = self.__urlencode_json("hostname")
        query_params = {}
        data = self.__get_json("get_chart_data_ungrouped", uri_params, query_params)
        self.assertTrue(len(data) == 1)
        self.assertTrue("name" in data[0])
        self.assertTrue("data" in data[0])
        self.assertTrue(len(data[0]["data"]) > 0)

    def test_get_scatter_data(self):
        uri_params = OrderedDict()
        uri_params[u"project"] = "vicenter"
        uri_params[u"tablename"] = "hostSystemDiskStats"
        uri_params[u"datestring"] = "2015-07-13"
        uri_params[u"keys"] = self.__urlencode_json(["vmwsr2impax1.tilak.cc","naa.6000d7760000a34d70fef71348bb8f91"])
        uri_params[u"value_keys"] = self.__urlencode_json(["disk.totalReadLatency.average","disk.totalWriteLatency.average"])
        uri_params[u"datatype"] = self.__urlencode_json("absolute")
        uri_params[u"group"] = self.__urlencode_json("hostname")
        query_params = {}
        data = self.__get_json("get_scatter_data", uri_params, query_params)
        print data
        self.assertTrue(len(data) > 1)
        self.assertTrue(type(data) == list)
        self.assertTrue(type(data[0]) == dict)



if __name__ == "__main__":
    unittest.main()
    test_get_vicenter_timeseries()
