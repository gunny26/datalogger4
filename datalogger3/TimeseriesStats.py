#!/usr/bin/python
# pylint: disable=line-too-long
"""
Modules deals with timeseries statistics
"""
import json
import logging
# own modules
from datalogger3.CustomExceptions import *


def mean(data):
    """Return the sample arithmetic mean of data."""
    n = len(data)
    if n < 1:
        raise ValueError('mean requires at least one data point')
    return sum(data)/float(n) # in Python 2 use sum(data)/float(n)

def _ss(data):
    """Return sum of square deviations of sequence data."""
    c = mean(data)
    ss = sum((x-c)**2 for x in data)
    return ss

def pstdev(data):
    """Calculates the population standard deviation."""
    n = len(data)
    if n < 2:
        raise ValueError('variance requires at least two data points')
    ss = _ss(data)
    pvar = ss/n # the population variance
    return pvar**0.5

def median(data):
    """Calculate median value of list"""
    half = len(data) // 2
    data.sort()
    if not len(data) % 2:
        return (data[half - 1] + data[half]) / 2.0
    return data[half]

def geometric_mean(nums):
    '''
        Return the geometric average of nums
        @param    list    nums    List of nums to avg
        @return   float   Geometric avg of nums
    '''
    return (reduce(lambda x, y: x*y, nums))**(1.0/len(nums))

def increments(data):
    """
    sums up all incrementing steps in list
    so:
    if x2 > x1 : increments += (x2-x1)
    else : do nothin
    """
    return float(sum([data[index + 1] - data[index] for index in range(len(data)-1) if data[index + 1] > data[index]]))

def decrements(data):
    """
    sums up all decrementing steps in list
    so:
    if x1 < x2 : decrements += (x1-x2)
    else : do nothin

    value will be always positive
    """
    return float(sum([data[index] - data[index + 1] for index in range(len(data)-1) if data[index + 1] < data[index]]))


class TimeseriesStats(object):
    """
    Statistics for one sepcific Timeseries Object

    separated to cache statistics in own files
    """
    stat_funcs = {
        "min" : min,
        "max" : max,
        "avg" : mean,
        "sum" : sum,
        "std" : pstdev,
        "median" : lambda series: median(list(series)),
        "count" : len,
        "first" : lambda series: series[0],
        "last" : lambda series: series[-1],
        "mean" : mean,
        "inc" : increments,
        "dec" : decrements,
        "diff" : lambda series: series[-1] - series[0],
    }

    def __init__(self, timeseries):
        """
        creates some statistical values for given timeseries object
        statistical values are listed in TimeseriesStats.funcnames
        for every column in Timeseries Object these statistical values are calculated

        parameters:
        timeseries <Timeseries>
        """
        # define new data
        self.__stats = {}
        # calculate statisticsi, if timeseries given
        for key in timeseries.headers:
            series = timeseries.get_serie(key)
            if len(series) == 0:
                logging.error("%s %s", key, len(timeseries))
                raise TimeseriesEmptyError("Timeseries without data cannot have statistics")
            elif len(series) > 1:
                self.__stats[key] = {}
                for func_name, func in self.stat_funcs.items():
                    self.__stats[key][func_name] = func(series)
            elif len(series) == 1: # special case if there is only one value a day
                self.__stats[key] = {
                    "min" : series[0],
                    "max" : series[0],
                    "avg" : series[0],
                    "sum" : series[0],
                    "std" : 0.0,
                    "median" : series[0],
                    "count" : 1,
                    "first" : series[0],
                    "last" : series[0],
                    "mean" : series[0],
                    "inc" : 0.0,
                    "dec" : 0.0,
                    "diff" : 0.0,
                }

    def __eq__(self, other):
        """ test for equality in depth"""
        try:
            assert self.stat_funcs == other.stat_funcs
        except AssertionError as exc:
            logging.exception(exc)
            logging.error(self.stat_funcs)
            logging.error(other.stat_funcs)
            return False
        try:
            assert len(self.__stats.keys()) == len(other.stats.keys())
        except AssertionError as exc:
            logging.exception(exc)
            logging.error(self.__stats.keys())
            logging.error(other.stats.keys())
            return False
        try:
            assert all((key in other.stats.keys() for key in self.__stats.keys()))
        except AssertionError as exc:
            logging.exception(exc)
            logging.error(self.__stats.keys())
            logging.error(other.stats.keys())
            return False
        for key in self.__stats.keys():
            for funcname in self.__stats[key].keys():
                try:
                    assert self.__stats[key][funcname] == other.stats[key][funcname]
                except AssertionError as exc:
                    logging.exception(exc)
                    logging.error("[%s][%s] : %s != %s", key, funcname, self.__stats[key][funcname], other[key][funcname])
                    return False
        try:
            assert self.__stats == other.stats
        except AssertionError as exc:
            logging.exception(exc)
            logging.error(self.__stats)
            logging.error(other.stats)
            return False
        return True

    def __getitem__(self, index):
        """either numerical index or tuple (value_keyname, statistical functio name)"""
        if isinstance(index, tuple):
            value_key, func_name = index
            return self.__stats[value_key][func_name]
        return self.__stats[index]

    def __str__(self):
        """printable string representation"""
        return json.dumps(self.__stats, indent=4)
        outbuffer = []
        headers = [u"key", ] + list(self.stat_funcs.keys())
        outbuffer.append("\t".join(headers))
        for key in self.__stats.keys():
            row = (key, ) + tuple(("%02f" % self.stats[key][funcname] for funcname in self.stat_funcs.keys()))
            outbuffer.append("\t".join(row))
        return "\n".join(outbuffer)

    def keys(self):
        """dict behaviour"""
        return self.__stats.keys()

    def values(self):
        """dict behaviour"""
        return self.__stats.values()

    def items(self):
        """dict behaviour"""
        return self.__stats.items()

    @property
    def stats(self):
        """get statistics dictionary"""
        return self.__stats

    @stats.setter
    def stats(self, value):
        """set statistics dictionary"""
        self.__stats = value

    @property
    def funcnames(self):
        """get statistical function names"""
        return sorted(self.stat_funcs.keys())

    @classmethod
    def get_stat_func_names(cls):
        """get statistical function names"""
        return sorted(cls.stat_funcs.keys())

    def get_stats(self):
        """
        return statistics of timeseries object, and if is_dirty is True recreate statistics
        otherwise return stored data

        returns:
        <dict> value_keynames
            <dict> func
                <float> dunc(Timeseries)
        """
        return self.__stats

    def get_stat(self, stat_func_name):
        """
        func <str> func must be one of self.stat_funcs.keys()
        get one specific stat value for all value_keys

        returns:
        <dict> value_keyname
            <float> func(Timeseries)
        """
        try:
            assert stat_func_name in self.stat_funcs.keys()
        except AssertionError as exc:
            logging.error("func could be only one of %s", self.stat_funcs.keys())
            raise exc
        data = {}
        for key in self.__stats.keys():
            data[key] = self.__stats[key][stat_func_name]
        return data

    def dump(self, filehandle):
        """
        write internal data to filehandle in json format

        parameters:
        filehandle <file>

        returns:
        <None>
        """
        json.dump(self.__stats, filehandle)
        filehandle.flush()

    @staticmethod
    def load(filehandle):
        """
        recreate TimeseriesStats Object from stored JSON Data in filehandle

        parameters:
        filehandle <file>

        returns:
        <TimeseriesStats>
        """
        tsstats = TimeseriesStats.__new__(TimeseriesStats)
        tsstats.__stats = json.load(filehandle)
        return tsstats

    def to_json(self):
        """
        return json encoded statistics dictionary
        used to store data in file
        """
        return json.dumps(self.__stats)

    def to_data(self):
        """
        return data used to further encode via json
        """
        return self.__stats

    @staticmethod
    def from_json(jsondata):
        """
        create class from json encoded statistics dictionary
        """
        tsstats = TimeseriesStats.__new__(TimeseriesStats)
        tsstats.__stats = json.loads(jsondata)
        return tsstats
