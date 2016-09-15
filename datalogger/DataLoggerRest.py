#!/usr/bin/python

import urllib
import urllib2
from collections import OrderedDict as OrderedDict
import json
import base64
import unittest
import logging
import os
import requests
import calendar
import datetime
# own modules
from datalogger import Timeseries as Timeseries
from datalogger import TimeseriesArray as TimeseriesArray
from datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger import TimeseriesStats as TimeseriesStats
from datalogger import QuantileArray as QuantileArray

DATALOGGER_URL = "http://srvmgdata1.tilak.cc/rest/v2"

class DataLoggerRest(object):
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

    def get_projects(self):
        """
        get list of available projects

        returns:
        <list>
        """
        response = requests.get(self.__datalogger_url + "/")
        data = response.json()
        return data["projects"]

    def get_stat_func_names(self):
        """
        get list of available projects

        returns:
        <list>
        """
        response = requests.get(self.__datalogger_url + "/")
        data = response.json()
        return data["stat_func_names"]

    def get_last_businessday_datestring(self):
        """
        get list of available projects

        returns:
        <list>
        """
        response = requests.get(self.__datalogger_url + "/")
        data = response.json()
        return data["last_businessday_datestring"]

    def get_tablenames(self, project):
        """
        get list of available projects

        returns:
        <list>
        """
        response = requests.get(self.__datalogger_url + "/" + project)
        data = response.json()
        return data

    def get_meta(self, project, tablename):
        """
        get list of available projects

        returns:
        <list>
        """
        response = requests.get(self.__datalogger_url + "/" + project +  "/" + tablename)
        data = response.json()
        return data

    def get_cache(self, project, tablename, datestring):
        """
        get list of available projects

        returns:
        <list>
        """
        response = requests.get(self.__datalogger_url + "/" + project +  "/" + tablename + "/" + datestring)
        data = response.json()
        return data

    def get_ts_index_keys(self, project, tablename, datestring):
        cache = self.get_cache(project, tablename, datestring)
        return cache["ts"]["keys"]

    def get_ts(self, project, tablename, datestring, index_key):
        """
        get list of available projects

        parameters:
        project <basestring>
        tablename <basestring>
        datestein <basestring>
        index_key <basestring> base64 encoded set or set

        returns:
        <list>
        """
        index_key_b64 = index_key
        if isinstance(index_key, tuple):
            index_key_b64 = base64.b64encode(unicode(index_key))
        response = requests.get(self.__datalogger_url + "/" + project +  "/" + tablename + "/" + datestring + "/ts/" + index_key_b64)
        data = response.json()
        return data

    def get_tsastat(self, project, tablename, datestring, func_name=None):
        """
        get list of available projects

        parameters:
        project <basestring>
        tablename <basestring>
        datestring <basestring>

        returns:
        <list>
        """
        url = self.__datalogger_url + "/" + project +  "/" + tablename + "/" + datestring + "/tsastat"
        if func_name is not None:
            url += "/" + func_name
        response = requests.get(url)
        data = response.json()
        return data

    def get_quantile(self, project, tablename, datestring):
        """
        get list of available projects

        parameters:
        project <basestring>
        tablename <basestring>
        datestring <basestring>

        returns:
        <list>
        """
        response = requests.get(self.__datalogger_url + "/" + project +  "/" + tablename + "/" + datestring + "/quantile")
        data = response.json()
        return data

    def get_monthstats(self, project, tablename, monthstring):
        """
        return monthly overall statistics

        this will return the full dataset, even if you need only one particular
        index_key/value_key/stat_func_name combination
        """
        stats = {}
        for datestring in self.monthwalker(monthstring):
            # if there is no stored tsastat skip this datestring
            if len(self.get_cache(project, tablename, datestring)["tsastat"]["keys"]) > 0:
                tsastat = self.get_tsastat(project, tablename, datestring)
                index_keynames = tsastat[0]
                value_keynames = tsastat[1]
                stats[datestring] = {}
                for item in tsastat[2]:
                    stats[datestring][tuple(item[0])] = eval(item[1])
        return stats

    def get_yearstats(self, project, tablename, yearstring, index_key, value_key):
        """
        return monthly overall statistics

        this will return the full dataset, even if you need only one particular
        index_key/value_key/stat_func_name combination
        """
        stats = {}
        for datestring in self.datewalker("%s-01-01" % yearstring, "%s-12-31" % yearstring):
            # if there is no stored tsastat skip this datestring
            if len(self.get_cache(project, tablename, datestring)["tsastat"]["keys"]) > 0:
                tsastat = self.get_tsastat(project, tablename, datestring)
                index_keynames = tsastat[0]
                # skip day, of there is no such index_key
                value_keynames = tsastat[1]
                stats[datestring] = {
                    index_key: None
                    }
                for item in tsastat[2]:
                    # format: [
                    #            [
                    #            tuple(index_key), {
                    #                value_key : statistical values
                    #                }
                    #            ],
                    #           next index_key
                    #           ]
                    # store only data necessary
                    if tuple(item[0]) == index_key:
                        logging.error("found: %s", eval(item[1])[value_key])
                        stats[datestring][tuple(item[0])] = eval(item[1])[value_key]
        return stats

    @staticmethod
    def datestring_to_date(datestring):
        """function to convert datestring to datetime object"""
        year, month, day = datestring.split("-")
        return datetime.date(int(year), int(month), int(day))

    @classmethod
    def datewalker(cls, datestring_start, datestring_stop):
        """
        function to walk from beginning datestring to end datestring,
        in steps of one day
        """
        start_date = cls.datestring_to_date(datestring_start)
        stop_date = cls.datestring_to_date(datestring_stop)
        while start_date <= stop_date:
            yield start_date.isoformat()
            start_date = start_date + datetime.timedelta(days=1)

    @classmethod
    def monthwalker(cls, monthdatestring):
        """
        funtion to walk from first day to last day in given month
        """
        year, month = monthdatestring.split("-")
        lastday = calendar.monthrange(int(year), int(month))[1]
        start = "%04d-%02d-01" % (int(year), int(month))
        stop = "%04d-%02d-%02d" % (int(year), int(month), lastday)
        return cls.datewalker(start, stop)

class Test(unittest.TestCase):

    datalogger = DataLoggerRest("http://srvmgdata1.tilak.cc/rest/v2")

    def test_projects(self):
        data = self.datalogger.get_projects()
        #logging.error(data)
        self.assertTrue(isinstance(data, list))
        assert u"ucs" in data

    def test_stat_func_names(self):
        data = self.datalogger.get_stat_func_names()
        #logging.error(data)
        self.assertTrue(isinstance(data, list))
        assert u"max" in data

    def test_last_businessday_datestring(self):
        data = self.datalogger.get_last_businessday_datestring()
        #logging.error(data)
        self.assertTrue(isinstance(data, basestring))

    def test_caches(self):
        datestring = "2016-01-01"
        for project in self.datalogger.get_projects():
            tablenames = self.datalogger.get_tablenames(project)
            #logging.error(tablenames)
            for tablename in tablenames:
                cache = self.datalogger.get_cache(project, tablename, datestring)
                #logging.error(cache)
                index_keys = self.datalogger.get_ts_index_keys(project, tablename, datestring)
                #logging.error(index_keys.keys())

    def test_tablenames(self):
        for project in self.datalogger.get_projects():
            tablenames = self.datalogger.get_tablenames(project)
            #logging.error(tablenames)
            for tablename in tablenames:
                tabledata = self.datalogger.get_meta(project, tablename)
                #logging.error(tabledata)

    def test_ts(self):
        project = "glt"
        tablename = "energiemonitor"
        datestring = "2016-08-01"
        index_key = (u'LKI', u'Strom_LKI_Gesamt')
        index_key_b64 = "KHUnTEtJJywgdSdTdHJvbV9MS0lfR2VzYW10Jyk="
        ts1 = self.datalogger.get_ts(project, tablename, datestring, index_key)
        #logging.error(ts1)
        ts2 = self.datalogger.get_ts(project, tablename, datestring, index_key_b64)
        self.assertEqual(ts1, ts2)
        tsastat = self.datalogger.get_tsastat(project, tablename, datestring)
        #logging.error(tsastat)
        tsastat_max = self.datalogger.get_tsastat(project, tablename, datestring, "max")
        #logging.error(tsastat_max)
        quantile = self.datalogger.get_quantile(project, tablename, datestring)
        #logging.error(quantile)

    def test_monthly(self):
        project = "glt"
        tablename = "energiemonitor"
        monthstring = "2016-07"
        index_key = (u'LKI', u'Strom_LKI_Gesamt')
        value_key = "wert"
        stat_func_name = "max"
        stats = self.datalogger.get_monthstats(project, tablename, monthstring)
        for datestring, data in stats.items():
            logging.error("%s : %d", datestring, data[index_key][value_key][stat_func_name])

    def test_yearly(self):
        project = "ipstor"
        tablename = "vrsClientTable"
        yearstring = "2016"
        index_key = (u'vsanapp3', u'VMWSR226', u'VMWARE_PROD3_SATA_RAW_srvapbrain')
        value_key = "vrsclReadError"
        stat_func_name = "max"
        stats = self.datalogger.get_yearstats(project, tablename, yearstring, index_key, value_key)
        for datestring, data in stats.items():
            if data[index_key] is not None:
                logging.error("%s : %d", datestring, data[index_key][stat_func_name])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
