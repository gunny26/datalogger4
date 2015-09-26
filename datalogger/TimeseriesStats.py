#!/usr/bin/python
import numpy
import json
#from scipy import stats
import logging


class TimeseriesStats(object):
    """
    Statistics for one sepcific Timeseries Object

    separated to cache statistics in own files
    """
    stat_funcs = {
        u"min" : lambda series: numpy.min(series),
        u"max" : lambda series: numpy.max(series),
        u"avg" : lambda series: numpy.average(series),
        u"sum" : lambda series: numpy.sum(series),
        u"std" : lambda series: numpy.std(series),
        u"median" : lambda series: numpy.median(series),
        u"count" : lambda series: len(series),
        u"first" : lambda series: series[0],
        u"last" : lambda series: series[-1],
        u"mean" : lambda series: numpy.mean(series),
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
            if len(series) > 0:
                self.__stats[key] = {}
                for func_name, func in self.stat_funcs.items():
                    self.__stats[key][func_name] = func(series)
            else:
                logging.error("%s %s", key, len(timeseries))
                raise StandardError("Timeseries without data cannot have stats")

    def __eq__(self, other):
        """ test for equality ion depth"""
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
        return self.__stats[index]

    def keys(self):
        return self.__stats.keys()

    def values(self):
        return self.__stats.values()

    def items(self):
        return self.__stats.items()

    @property
    def stats(self):
        return self.__stats

    @stats.setter
    def stats(self, value):
        self.__stats = value

    @property
    def funcnames(self):
        return self.stat_funcs.keys()

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

    def get_stat(self, funcname):
        """
        func <str> func must be one of self.stat_funcs.keys()
        get one specific stat value for all value_keys

        returns:
        <dict> value_keyname
            <float> func(Timeseries)
        """
        try:
            assert funcname in self.stat_funcs.keys()
        except AssertionError as exc:
            logging.error("func could be only one of %s", self.stat_funcs.keys())
            raise exc
        data = {}
        for key in self.__stats.keys():
            data[key] = self.__stats[key][funcname]
        return data

    def dump(self, filehandle):
        """
        writer internal data to filehandle in json format

        parameters:
        filehandle <file>

        returns:
        <None>
        """
        json.dump(self.__stats, filehandle)

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
