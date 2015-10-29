#!/usr/bin/python
import json
import base64
import os
import logging
# own modules
from TimeseriesStats import TimeseriesStats as TimeseriesStats
from CustomExceptions import *

class TimeseriesArrayStats(object):
    """
    hold dictionary of TimeseriesStats objects
    """

    def __init__(self, tsa):
        """
        creates TimeseriesStat objects for every key in given TimeseriesArray object
        index_keys and value_keys are used from given TimeseriesArray object

        parameters:
        tsa <TimeseriesArray>
        """
        # define instance data
        self.__stats = {}
        self.__index_keys = tuple([unicode(value) for value in tsa.index_keys])
        self.__value_keys = tuple([unicode(value) for value in tsa.value_keys])
        for key in tsa.keys():
            try:
                self.__stats[key] = TimeseriesStats(tsa[key])
            except TimeseriesEmptyError as exc:
                logging.info("Timeseries for key %s is length zero, skipping", key)

    def __eq__(self, other):
        try:
            assert self.__index_keys == other.index_keys
            assert self.__value_keys == other.value_keys
            assert len(self.__stats.keys()) == len(other.stats.keys())
            for key in self.__stats.keys():
                assert self.__stats[key] == other.stats[key]
        except AssertionError as exc:
            logging.exception(exc)
            return False
        return True

    def __len__(self):
        return len(self.__stats.keys())

    def __getitem__(self, key):
        return self.__stats[key]

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
    def index_keys(self):
        return self.__index_keys

    @index_keys.setter
    def index_keys(self, value):
        self.__index_keys = value

    @property
    def value_keys(self):
        return self.__value_keys

    @value_keys.setter
    def value_keys(self, value):
        self.__value_keys = value

    def get_stats(self, value_key):
        """
        returns dictionary of stats of every Timeseries object in Array for this
        specific value_key only

        parameters:
        value_key <str> must be in self.value_keys

        returns:
        <dict>
        """
        assert value_key in self.__value_keys
        ret_data = {}
        for key, t_stat in self.__stats.items():
            ret_data[key] = t_stat.stats[value_key]
        return ret_data

    @staticmethod
    def get_tsstat_dumpfilename(key):
        """
        create filename for stored or to be stored TimeseriesStats objects
        from given key
        key will be base64 encoded

        parameters:
        key <tuple>

        returns:
        <str>
        """
        return "tsstat_%s.json" % base64.urlsafe_b64encode(unicode(key))

    @staticmethod
    def get_dumpfilename(index_keys):
        """
        create filename for stored or to be stored TimeseriesArrayStats
        from given index_keys
        index_keys will be base64 encoded

        parameters:
        index_keys <tuple>

        returns:
        <str>
        """
        return "tsastat_%s.json" % base64.urlsafe_b64encode(unicode(index_keys))

    def dump(self, outpath, overwrite=False):
        """
        dump internal data to json file
        the filename is automatically created from index_keys

        parameters:
        outpath <str> path wehere json file will be placed
        overwrite <bool> wheter or not a existing file should be overwritten
        """
        #logging.info("index_keys: %s", self.__index_keys)
        outfilename = os.path.join(outpath, self.get_dumpfilename(self.__index_keys))
        outdata = {
            "index_keys" : self.__index_keys,
            "value_keys" : self.__value_keys,
            "tsstat_filenames" : []
        }
        for key, tsstats in self.__stats.items():
            filename = self.get_tsstat_dumpfilename(key)
            fullfilename = os.path.join(outpath, filename)
            if (not os.path.isfile(fullfilename)) or (overwrite is True):
                filehandle = open(fullfilename, "wb")
                tsstats.dump(filehandle)
                filehandle.close()
            outdata["tsstat_filenames"].append(filename)
        json.dump(outdata, open(outfilename, "wb"))

    @staticmethod
    def filtermatch(key_dict, filterkeys, matchtype):
        """
        key_dict is the whole index key, aka
        {hostname : test, instance:1, other:2}

        filterkey is part
        {hostname : test}
        """
        assert matchtype in ("and", "or")
        matched = 0
        for key in filterkeys.keys():
            if key_dict[key] == filterkeys[key]:
                matched += 1
        # every key must match at AND
        if (matchtype == "and") and (matched == len(filterkeys.keys())):
            return True
        # at least one key must match at OR
        elif (matchtype == "or") and (matched > 0):
            return True
        return False

    @staticmethod
    def get_load_filenames(path, index_keys, filterkeys=None, matchtype="and"):
        """
        filterkeys could be a part of existing index_keys
        all matching keys will be used
        """
        tsastat_filename = TimeseriesArrayStats.get_dumpfilename(index_keys)
        logging.debug("tsastat_filename: %s", tsastat_filename)
        infile = os.path.join(path, tsastat_filename)
        data = json.load(open(infile, "rb"))
        logging.debug("loaded json data")
        logging.debug("index_keys: %s", data["index_keys"])
        logging.debug("value_keys: %s", data["value_keys"])
        logging.debug("number of ts files: %s", len(data["tsstat_filenames"]))
        filenames = {}
        for filename in data["tsstat_filenames"]:
            logging.debug("reading key for tsstat from file %s", filename)
            enc_key = filename.split(".")[0][7:] # only this pattern tsstat_(.*).json
            key = eval(base64.urlsafe_b64decode(str(enc_key))) # must be str not unicode
            key_dict = dict(zip(index_keys, key))
            if filterkeys is not None:
                if TimeseriesArrayStats.filtermatch(key_dict, filterkeys, matchtype):
                    logging.debug("adding tsastat key : %s", key)
                    filenames[key] = os.path.join(path, filename)
            else:
                # no filterkeys means every file is added
                logging.debug("adding tsa key : %s", key)
                filenames[key] = os.path.join(path, filename)
        return filenames

    @staticmethod
    def load(path, index_keys, filterkeys=None, matchtype="and"):
        """
        load stored json file (with dump() created) and return TimeseriesArrayStats object

        parameters:
        path <str> path to search for stored json file, the filename is automatically created from given index_keys
        index_keys <tuple> list of index_keys

        returns:
        <TimeseriesArray>
        """
        #logging.info("index_keys: %s", index_keys)
        infilename = os.path.join(path, TimeseriesArrayStats.get_dumpfilename(index_keys))
        try:
            indata = json.load(open(infilename, "rb"))
        except StandardError as exc:
            logging.exception(exc)
            logging.error("something went wrong while loading %s", infilename)
            raise exc
        #logging.info("loaded JSON data: %s", indata)
        tsastats = TimeseriesArrayStats.__new__(TimeseriesArrayStats)
        tsastats.__index_keys = tuple(indata["index_keys"])
        tsastats.__value_keys = tuple(indata["value_keys"])
        tsastats.__stats = {}
        #for filename in indata["tsstat_filenames"]:
        for key, filename in tsastats.get_load_filenames(path, index_keys, filterkeys, matchtype).items():
            #logging.info("loading TimeseriesStats object from %s", fullfilename)
            filehandle = open(filename, "rb")
            tsastats.__stats[key] = TimeseriesStats.load(filehandle)
            filehandle.close()
        return tsastats
