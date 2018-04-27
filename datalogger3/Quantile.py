#!/usr/bin/pypy
# pylint: disable=line-too-long
import json
import os
import logging
# own modules
from datalogger3.CustomExceptions import *
from datalogger3.TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats


class QuantileArray(object):
    """
    hold a number of Quantile Objects
    """
    __filename = "quantile.json"

    def __init__(self, tsa, tsastats=None):
        """
        the data will be calculated imediately

        parameters:
        tsa <TimeseriesArray>
        """
        self.__data = {}
        self.__keys = tuple(tsa.keys())
        self.__value_keynames = tuple(tsa.value_keynames)
        for value_keyname in self.__value_keynames:
            try:
                self.__data[value_keyname] = Quantile(tsa, value_keyname, tsastats=tsastats)
            except QuantileError as exc:
                logging.exception(exc)
                logging.error("skipping value_key %s", value_keyname)

    def keys(self):
        """all available index_keys"""
        return self.__keys

    @property
    def value_keynames(self):
        """all available value_keynames"""
        return self.__value_keynames

    def __str__(self):
        return json.dumps(self.to_data(), indent=4)

    def __getitem__(self, key):
        """
        overloaded __getitem__

        key <tuple> : returns every quantille, for every value_key of this key
        returns: <dict>

        key <basestring> : return quantille for this value_key for every key available
        returns: <Quantile>
        """
        if isinstance(key, tuple):
            return dict(((value_keyname, self.__data[value_keyname][key]) for value_keyname in self.__value_keynames))
        elif isinstance(key, str):
            return self.__data[key]
        else:
            raise KeyError("key %s not found" % key)

    def __eq__(self, other):
        """test for equality"""
        try:
            assert type(self) == type(other)
            assert self.__keys == other.keys
            assert self.__value_keynames == other.value_keynames
            for key in self.__data.keys():
                assert self.__data[key] == other[key]
            return True
        except AssertionError as exc:
            logging.debug("%s, %s", self.__keys, other.keys)
            logging.debug("%s, %s", self.__value_keynames, other.value_keynames)
            logging.exception(exc)
            return False

    @classmethod
    def get_dumpfilename(cls, outdir):
        return os.path.join(outdir, cls.__filename)

    def dump(self, outdir):
        """
        dump internal data to filehandle
        """
        quantille_data = dict(((key, quantille.dumps()) for key, quantille in self.__data.items()))
        with open(os.path.join(outdir, self.__filename), "wt") as outfile:
            json.dump((quantille_data, self.__keys, self.__value_keynames), outfile)

    def to_data(self):
        return {
            "keys" : self.__keys,
            "value_keynames" : self.__value_keynames
        }

    def to_json(self):
        """
        dump internal data to json
        """
        quantille_data = dict(((key, quantille.dumps()) for key, quantille in self.__data.items()))
        return json.dumps((quantille_data, self.__keys, self.__value_keynames))

    @classmethod
    def load(cls, outdir):
        """
        load data from stores json file on disk

        parameter:
        filehandle <file> to read from, json expected
        """
        qa = QuantileArray.__new__(QuantileArray)
        with open(os.path.join(outdir, cls.__filename), "rt") as infile:
            quantille_data, qa.__keys, qa.__value_keynames = json.load(infile)
        # convert to tuple, to be equal to normal initialization
        qa.__keys = tuple((tuple(key) for key in qa.__keys))
        qa.__value_keynames = tuple(qa.__value_keynames)
        qa.__data = dict(((key, Quantile.loads(data)) for key, data in quantille_data.items()))
        return qa

    @staticmethod
    def from_json(json_data):
        """
        load data from json encoded string

        parameters:
        json_data <basestring> json encoded
        """
        qa = QuantileArray.__new__(QuantileArray)
        quantille_data, qa.__keys, qa.__value_keynames = json.loads(json_data)
        # convert to tuple, to be equal to normal initialization
        qa.__keys = tuple((tuple(key) for key in qa.__keys))
        qa.__value_keynames = tuple(qa.__value_keynames)
        qa.__data = dict(((key, Quantile.loads(data)) for key, data in quantille_data.items()))
        return qa




class Quantile(object):
    """
    class to calulate and store quantile for one TimeseriesArray value_key
    """
    __quants = {
        0 : 0,
        1 : 0,
        2 : 0,
        3 : 0,
        4 : 0,
    }
    __width = 20

    def __init__(self, tsa, value_key, tsastats):
        """
        parameters:
        tsa <TimeseriesArray>
        value_key <str> must be a value_key of the Timeseries used in tsa
        maxx    <None> to autoscale max value upon all given Timeseries in tsa
                <float> for specific maximum value to use
        """
        self.__quantile = {}
        self.__sortlist = None
        if len(tsa) == 0:
            raise QuantileError("EmptyTsaException detected, not possible to calculate anything with nothing")
        # get min and max over all available timeseries
        self.__maxx = max((tsstats[value_key]["max"] for key, tsstats in tsastats.items()))
        self.__minn = min((tsstats[value_key]["min"] for key, tsstats in tsastats.items()))
        # do the calculations
        # width of each quantile
        width = int(100 / (len(self.__quants.keys()) - 1))
        # if __maxx or width is equal zero, empty Data
        if (self.__maxx == 0.0) or  (width == 0):
            self.__quantile = self.__quants.copy()
            logging.debug("either length of data or maximum is zero, so all quantile values will be zero")
        try:
            for key in tsa.keys():
                try:
                    # if there is no timeseriesstats value for this particular tsa.
                    # skip it
                    tsastats[key][value_key]
                    self.__quantile[key] = self.__calculate(tsa[key][value_key])
                except KeyError as exc:
                    logging.debug("no timeseriesstats available for index_key = %s and value_key = %s, skipping", key, value_key)
        except Exception as exc:
            logging.exception(exc)
            logging.error("an error occured with value_key %s", value_key)
            raise exc
        # self.sort() # do initial sort

    @property
    def quantile(self):
        """get internal data"""
        return self.__quantile

    def to_data(self):
        """get internal data"""
        return [[str(key), ] + list(value.values()) for key, value in self.__quantile.items()]

    @property
    def maxx(self):
        """get maximum overall Timeseries defined or calculated"""
        return self.__maxx

    def dumps(self):
        """
        dump object data to json encoded string
        """
        return json.dumps(str((self.__quantile, self.__maxx)))

    @staticmethod
    def loads(data):
        """
        recreate object from data string in json format
        """
        quantille = Quantile.__new__(Quantile)
        quantille.__quantile, quantille.__maxx = eval(json.loads(data))
        #quantille.sort()
        return quantille

    def __eq__(self, other):
        try:
            assert type(self) == type(other)
            assert self.__quantile == other.quantile
            assert self.__maxx == other.maxx
            return True
        except AssertionError as exc:
            logging.exception(exc)
            return False

    def __getitem__(self, key):
        return self.__quantile[key]

    def __calculate(self, series):
        """
        actually do the calculations
        """
        quants = self.__quants.copy()
        # TODO: Performance Optimization needed
        # the range from __minn to __maxx
        # __maxx and __minn both can be negative
        value_range = abs(self.__maxx - self.__minn)
        # if value_range is zero, skip calculations
        if value_range == 0.0:
            return quants
        for value in series:
            quant = (100 * abs(value - self.__minn) / value_range) / self.__width
            try:
                quants[int(quant)] += 1
            except KeyError as exc:
                # this is the case if value == __maxx
                if int(quant) == len(quants):
                    quants[int(quant) - 1] += 1
                else:
                    logging.error("quant = %s, value = %s, __maxx = %s, __minn = %s, width = %s", quant, value, self.__maxx, self.__minn, self.__width)
                    raise exc
        return quants

    def head(self, maxlines=10):
        """
        output head
        """
        outbuffer = []
        for index, key in enumerate(self.__sortlist[::-1]):
            outbuffer.append("%s : %s" % (str(key), str(self.__quantile[key])))
            if index == maxlines:
                break
        return "\n".join(outbuffer)

    def tail(self, maxlines=10):
        """
        output tail
        """
        outbuffer = []
        for index, key in enumerate(self.__sortlist):
            outbuffer.append("%s : %s" % (str(key), str(self.__quantile[key])))
            if index == maxlines:
                break
        return "\n".join(outbuffer[::-1])

    def __str__(self):
        """
        combine head and tail
        """
        outbuffer = []
        outbuffer.append("%d keys in dataset" % len(self.__quantile))
        outbuffer.append(self.head())
        outbuffer.append("...")
        outbuffer.append(self.tail())
        return "\n".join(outbuffer)

    def sort(self, quant=None):
        """
        sort on one specific quantile or on weighted value of all quants

        parameters:
        quant   <None> for weighted value 10^4*quant(4)* + 10^3*quant(3) + 10^2*quant(2) ...
                <int> for sort for specific quant

        returns:
        None
        """
        if quant is None: # sort bei weight
            self.__sortlist = [key for key, values in sorted(self.__quantile.items(), key=lambda items: sum((10^quantille * count for quantille, count in enumerate(items[1].values()))))]
        elif isinstance(quant, int):
            self.__sortlist = [key for key, values in sorted(self.__quantile.items(), key=lambda items: items[1][quant])]
