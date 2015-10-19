#!/usr/bin/pypy
import json
import logging

def get_mse(series1, series2):
    """
    the series as is
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
    sorted but not normalized values
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
    normalized values
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
    sorted and normalized
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


class CorrelationMatrixArray(object):

    def __init__(self, tsa):
        self.__data = {}
        for value_key in tsa.value_keys:
            print "calculating value_key %s" % value_key
            self.__data[value_key] = CorrelationMatrix(tsa, value_key)

    def __eq__(self, other):
        try:
            assert self.__data.keys() == other.keys()
            for key in self.__data.keys():
                assert self.__data[key] == other[key]
            return True
        except AssertionError as exc:
            logging.exception(exc)
            print self.__data.keys(), other.keys()
        return False

    def __getitem__(self, key):
        return self.__data[key]

    def keys(self):
        return self.__data.keys()

    def dump(self, filehandle):
        data = dict(((key, self.__data[key].dumps()) for key in self.__data.keys()))
        json.dump(data, filehandle)

    @staticmethod
    def load(filehandle):
        cma = CorrelationMatrixArray.__new__(CorrelationMatrixArray)
        data = json.load(filehandle)
        cma.__data = dict(((key, CorrelationMatrix.loads(data)) for key, data in data.items()))
        return cma


class CorrelationMatrix(object):

    def __init__(self, tsa, value_key):
        self.__data = self.__get_correlation_matrix(tsa, value_key)

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
        if isinstance(key, tuple):
            return self.__data[key[0]][key[1]]
        else:
            return self.__data[key]

    def keys(self):
        return self.__data.keys()

    @staticmethod
    def __get_correlation_matrix(tsa, value_key):
        """
        search for corelating series in all other series available
        """
        print "Searching for correlation in value_key %s)" % value_key
        matrix = {}
        keylist = tsa.keys()
        for key in keylist:
            series = tsa[key][value_key]
            matrix[key] = {}
            for otherkey in keylist:
                try:
                    matrix[key][otherkey]
                except KeyError:
                    if otherkey not in matrix:
                        matrix[otherkey] = {}
                    other = tsa[otherkey][value_key]
                    if len(series) == len(other):
                        matrix[key][otherkey] = get_mse_sorted_norm(series, other)
                        matrix[otherkey][key] = matrix[key][otherkey]
                    else:
                        print "skipping, dataseries are not of same length"
        return matrix

    def dumps(self):
        return json.dumps(str(self.__data))

    @staticmethod
    def loads(data):
        cm = CorrelationMatrix.__new__(CorrelationMatrix)
        cm.__data = eval(json.loads(data))
        return cm
