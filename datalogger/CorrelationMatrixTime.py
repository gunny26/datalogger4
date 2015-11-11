#!/usr/bin/pypy
"""
compare Timeseries Data in Time for the same value_keys

useful to identify some major differences,
by watching on the Timeseries with the highest MSE values

MSE - Mean Squared Error
"""
import json
import logging


def get_mse(series1, series2):
    """
    compare two series item by item
    series length has to be euqal
    """
    assert len(series1) == len(series2)
    mse = 0.0
    for index, data1 in enumerate(series1):
        diff = (data1 - series2[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_sorted(series1, series2):
    """
    compare two SORTED series item by item
    series length has to be euqal
    """
    assert len(series1) == len(series2)
    mse = 0.0
    series2_s = sorted(series2)
    for index, data1 in enumerate(sorted(series1)):
        diff = (data1 - series2_s[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_norm(series1, series2):
    """
    compare two NORMALIZED series item by item
    series length has to be euqal
    """
    assert len(series1) == len(series2)
    mse = 0.0
    max_v = max(max(series1), max(series2))
    s1 = tuple((value/max_v for value in series1))
    s2 = tuple((value/max_v for value in series2))
    for index, data1 in enumerate(s1):
        diff = (data1 - s2[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_sorted_norm(series1, series2):
    """
    compare two NORMALIZED and SORTED series item by item
    series length has to be euqal
    """
    assert len(series1) == len(series2)
    mse = 0.0
    max_v = max(series1)
    if max_v == 0.0:
        # difference is equa series2
        return sum((value * value for value in series2))/len(series1)
    s1 = tuple((value/max_v for value in sorted(series1)))
    s2 = tuple((value/max_v for value in sorted(series2)))
    for index, data1 in enumerate(s1):
        diff = (data1 - s2[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_sorted_norm_missing(series1, series2):
    """
    compare two NORMALIZED and SORTED series items by item,
    until one series is empty, than stop

    series length must not differ in more than 10 percent
    """
    mse = 0.0
    max_v = max(series1)
    if max_v == 0.0:
        # difference is equa series2
        return sum((value * value for value in series2))/len(series1)
    s1 = tuple((value/max_v for value in sorted(series1, reverse=True)))
    s2 = tuple((value/max_v for value in sorted(series2, reverse=True)))
    assert abs(len(s1) - len(s2)) / max(len(s1), len(s2)) < 0.1 # not more than 10% length difference
    for index, data1 in enumerate(s1):
        try:
            diff = (data1 - s2[index])
            mse += diff * diff
        except IndexError:
            break
    mse /= len(series1)
    return mse


class CorrelationMatrixTime(object):
    """
    for all available Timeseries Objects in TimeseriesArray
    compare the Timeseries with the Timeseries for this key on another date

    to find out, which Timeseries differ most
    """

    def __init__(self, dataloggerweb, project, tablename, datestring1, datestring2, value_key):
        self.__dataloggerweb = dataloggerweb
        self.__project = project
        self.__tablename = tablename
        self.__datestring1 = datestring1
        self.__datestring2 = datestring2
        self.__value_key = value_key
        self.__data = self.__get_correlation_matrix()

    @property
    def data(self):
        return self.__data

    def __eq__(self, other):
        try:
            assert self.__data == other.data
            return True
        except AssertionError as exc:
            logging.exception(exc)
            print self.__data.keys(), other.data.keys()
        return False

    def __getitem__(self, key):
        return self.__data[key]

    def keys(self):
        return self.__data.keys()

    def values(self):
        return self.__data.values()

    def items(self):
        return self.__data.items()

    def __get_correlation_matrix(self):
        """
        search for corelating series in all other series available
        """
        logging.info("getting caches")
        caches1 = self.__dataloggerweb.get_caches(self.__project, self.__tablename, self.__datestring1)
        caches2 = self.__dataloggerweb.get_caches(self.__project, self.__tablename, self.__datestring2)
        logging.info("Searching for correlation in value_key %s)", self.__value_key)
        matrix = {}
        for key_str in caches1["ts"]["keys"]:
            if key_str not in caches2["ts"]["keys"]:
                logging.debug("key %s is not in older tsa, skipping", str(key_str))
                continue
            key = eval(key_str)
            other = self.__dataloggerweb.get_ts(self.__project, self.__tablename, self.__datestring2, key)[key]
            series = self.__dataloggerweb.get_ts(self.__project, self.__tablename, self.__datestring1, key)[key]
            matrix[key] = get_mse_sorted_norm_missing(series[self.__value_key], other[self.__value_key])
        return matrix

    def dumps(self):
        """dump internal data to json"""
        return json.dumps(str(self.__data))

    @staticmethod
    def loads(data):
        """recreating object from jason encoded data"""
        cm = CorrelationMatrixTime.__new__(CorrelationMatrixTime)
        cm.__data = eval(json.loads(data))
        return cm
