#!/usr/bin/pypy
import json
import logging


class QuantillesArray(object):

    def __init__(self, tsa):
        self.__data = {}
        self.__keys = tuple(tsa.keys())
        self.__value_keys = tuple(tsa.value_keys)
        for value_key in self.__value_keys:
            try:
                self.__data[value_key] = Quantilles(tsa, value_key)
            except QuantillesError as exc:
                logging.exception(exc)
                logging.error("skipping value_key %s", value_key)

    @property
    def keys(self):
        return self.__keys

    @property
    def value_keys(self):
        return self.__value_keys

    def __getitem__(self, key):
        """
        overloaded __getitem__

        key <tuple> : returns every quantille, for every value_key of this key
        returns: <dict>

        key <basestring> : return quantille for this value_key for every key available
        returns: <Quantilles>
        """
        if isinstance(key, tuple):
            return dict(((value_key, self.__data[value_key][key]) for value_key in self.__value_keys))
        elif isinstance(key, basestring):
            return self.__data[key]
        else:
            raise KeyError("key %s not found" % key)

    def __eq__(self, other):
        try:
            assert type(self) == type(other)
            assert self.__keys == other.keys
            assert self.__value_keys == other.value_keys
            for key in self.__data.keys():
                assert self.__data[key] == other[key]
            return True
        except AssertionError as exc:
            logging.debug("%s, %s", self.__keys, other.keys)
            logigng.debug("%s, %s", self.__value_keys, other.value_keys)
            logging.exception(exc)
            return False

    def dump(self, filehandle):
        """
        dump internal data to filehandle
        """
        quantille_data = dict(((key, quantille.dumps()) for key, quantille in self.__data.items()))
        json.dump((quantille_data, self.__keys, self.__value_keys), filehandle)

    def to_json(self):
        """
        dump internal data to json
        """
        quantille_data = dict(((key, quantille.dumps()) for key, quantille in self.__data.items()))
        return json.dumps((quantille_data, self.__keys, self.__value_keys))

    @staticmethod
    def load(filehandle):
        qa = QuantillesArray.__new__(QuantillesArray)
        quantille_data, qa.__keys, qa.__value_keys = json.load(filehandle)
        # convert to tuple, to be equal to normal initialization
        qa.__keys = tuple((tuple(key) for key in qa.__keys))
        qa.__value_keys = tuple(qa.__value_keys)
        qa.__data = dict(((key, Quantilles.loads(data)) for key, data in quantille_data.items()))
        return qa

    @staticmethod
    def from_json(json_data):
        qa = QuantillesArray.__new__(QuantillesArray)
        quantille_data, qa.__keys, qa.__value_keys = json.loads(json_data)
        # convert to tuple, to be equal to normal initialization
        qa.__keys = tuple((tuple(key) for key in qa.__keys))
        qa.__value_keys = tuple(qa.__value_keys)
        qa.__data = dict(((key, Quantilles.loads(data)) for key, data in quantille_data.items()))
        return qa

class QuantillesError(Exception):
    """raised if there is some problem calculating Quantilles"""


class Quantilles(object):
    """
    class to calulate and store quantilles for one TimeseriesArray value_key
    """
    __quants = {
        0 : 0,
        1 : 0,
        2 : 0,
        3 : 0,
        4 : 0,
    }

    def __init__(self, tsa, value_key, maxx=None):
        """
        parameters:
        tsa <TimeseriesArray>
        value_key <str> must be a value_key of the Timeseries used in tsa
        maxx    <None> to autoscale max value upon all given Timeseries in tsa
                <float> for specific maximum value to use
        """
        self.__quantilles = {}
        self.__sortlist = None
        if maxx is None:
            self.__maxx = max((max(ts[value_key]) for key, ts in tsa.items()))
        else:
            self.__maxx = maxx
        # do the calculations
        width = int(100 / (len(self.__quants.keys()) - 1))
        # if __maxx or width is equal zero, empty Data
        if (self.__maxx == 0.0) or  (width == 0):
            raise QuantillesError("either length of data or maximum is zero")
        self.__quantilles = dict(((key, self.__calculate(ts[value_key], width)) for key, ts in tsa.items()))
        #for key, ts in tsa.items():
        #    self.__quantilles[key] =  self.__calculate(ts[value_key], width)
        self.sort() # do initial sort

    @property
    def quantilles(self):
        return self.__quantilles

    @property
    def maxx(self):
        return self.__maxx

    def dumps(self):
        """
        dump object data to json encoded string
        """
        return json.dumps(str((self.__quantilles, self.__maxx)))

    @staticmethod
    def loads(data):
        """
        recreate object from data string in json format
        """
        quantille = Quantilles.__new__(Quantilles)
        quantille.__quantilles, quantille.__maxx = eval(json.loads(data))
        quantille.sort()
        return quantille

    def __eq__(self, other):
        try:
            assert type(self) == type(other)
            assert self.__quantilles == other.quantilles
            assert self.__maxx == other.maxx
            return True
        except AssertionError as exc:
            print self.__maxx, other.maxx
            print self.__quantilles, other.quantilles
            logging.exception(exc)
            return False

    def __getitem__(self, key):
        return self.__quantilles[key]

    def __calculate(self, series, width):
        """
        actually do the calculations
        """
        quants = self.__quants.copy()
        for value in series:
            quant = int((100 * min(value, self.__maxx) / self.__maxx) / width)
            quants[quant] += 1
        return quants

    def head(self, maxlines=10):
        """
        output head
        """
        outbuffer = []
        for index, key in enumerate(self.__sortlist[::-1]):
            outbuffer.append("%s : %s" % (str(key), str(self.__quantilles[key])))
            if index == maxlines:
                break
        return "\n".join(outbuffer)

    def tail(self, maxlines=10):
        """
        output tail
        """
        outbuffer = []
        for index, key in enumerate(self.__sortlist):
            outbuffer.append("%s : %s" % (str(key), str(self.__quantilles[key])))
            if index == maxlines:
                break
        return "\n".join(outbuffer[::-1])

    def __str__(self):
        """
        combine head and tail
        """
        outbuffer = []
        outbuffer.append("%d keys in dataset" % len(self.__quantilles))
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
            self.__sortlist = [key for key, values in sorted(self.__quantilles.items(), key=lambda items: sum((10^quantille * count for quantille, count in enumerate(items[1].values()))))]
        elif isinstance(quant, int):
            self.__sortlist = [key for key, values in sorted(self.__quantilles.items(), key=lambda items: items[1][quant])]
