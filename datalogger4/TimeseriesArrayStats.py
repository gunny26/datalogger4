#!/usr/bin/python
# pylint: disable=line-too-long
"""
module to work with TimeseriesArrayStatistics

automatically calculates all TimeseriesStats for every Timeseries in TimeseriesArray
at initialization
"""
import sys
import json
import os
import logging
# own modules
from TimeseriesStats import TimeseriesStats
from CustomExceptions import *
from b64 import b64encode, b64decode, b64eval


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
        self.__autoload = {} # holding filenames to load key data
        self.__index_keynames = tuple(tsa.index_keynames)
        self.__value_keynames = tuple(tsa.value_keynames)
        for index_key in tsa.keys():
            try:
                self.__stats[index_key] = TimeseriesStats(tsa[index_key])
            except TimeseriesEmptyError as exc:
                logging.info("Timeseries for key %s is length zero, skipping", index_key)

    def __str__(self):
        return json.dumps(self.to_data(), indent=4, sort_keys=True)

    to_json = __str__

    def __eq__(self, other):
        try:
            assert self.__index_keynames == other.index_keynames
            assert self.__value_keynames == other.value_keynames
            assert len(self.keys()) == len(other.stats.keys())
            for key in self.keys():
                assert self[key] == other.stats[key]
        except AssertionError as exc:
            logging.exception(exc)
            return False
        return True

    def __len__(self):
        return len(self.keys())

    def __getitem__(self, key):
        """
        autoload tsstats if key is found and value is None
        """
        if self.__stats[key] is None:
            self.__stats[key] = self.__autoload_tsstats(self.__autoload[key])
        return self.__stats[key]

    def __delitem__(self, key):
        del self.__stats[key]

    def keys(self):
        return self.__stats.keys()

    def items(self):
        """mimic dict, but honor autoload feature"""
        return [(key, self[key]) for key in self.keys()]

    def values(self):
        """mimic dict, but honor autoload feature"""
        return [self[key] for key in self.__stats.keys()]

    @property
    def stats(self):
        return dict(self.items())

#    @stats.setter
#    def stats(self, value):
#        self.__stats = value

    @property
    def index_keynames(self):
        return self.__index_keynames

    @index_keynames.setter
    def index_keynames(self, value):
        self.__index_keynames = value

    @property
    def value_keynames(self):
        return self.__value_keynames

    @value_keynames.setter
    def value_keynames(self, value):
        self.__value_keynames = value

    def slice(self, value_keys):
        """
        remove all values_keys not in value_keys, and return new TimeseriesArrayStats object
        """
        assert all((value_key in self.__value_keynames for value_key in value_keys))
        outdata = []
        outdata.append(self.__index_keynames)
        outdata.append(value_keys)
        tsstat_data = []
        for key, tsstat in self.items():
            data = {}
            for value_key in value_keys:
                data[value_key] = tsstat[value_key]
            tsstat_data.append((key, json.dumps(data)))
        outdata.append(tsstat_data)
        new_tsastat = TimeseriesArrayStats.from_json(json.dumps(outdata))
        return new_tsastat

    def get_stats(self, value_key, stat_func_name=None):
        """
        returns dictionary of stats of every Timeseries object in Array for this
        specific value_key only

        parameters:
        value_key <str> must be in self.value_keys
        stat_func_name <str> must be in self.stat_func_names or None

        returns:
        <dict>
        """
        assert value_key in self.__value_keynames
        if stat_func_name is not None:
            assert stat_func_name in TimeseriesStats.get_stat_func_names()
        ret_data = {}
        for key, t_stat in self.items():
            if stat_func_name is not None:
                ret_data[key] = t_stat.stats[value_key][stat_func_name]
            else:
                ret_data[key] = t_stat.stats[value_key]
        return ret_data

    @staticmethod
    def _get_tsstat_dumpfilename(key):
        """
        create filename for stored or to be stored TimeseriesStats objects
        from given key
        key will be base64 encoded

        parameters:
        key <tuple>

        returns:
        <str>
        """
        return "tsstat_%s.json" % b64encode(key)

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
        return "tsastat_%s.json" % b64encode(index_keys)

    def dump(self, outpath, overwrite=False):
        """
        dump internal data to json file
        the filename is automatically created from index_keys

        parameters:
        outpath <str> path wehere json file will be placed
        overwrite <bool> wheter or not a existing file should be overwritten
        """
        #logging.info("index_keys: %s", self.__index_keynames)
        outfilename = os.path.join(outpath, self.get_dumpfilename(self.__index_keynames))
        outdata = {
            "index_keys" : self.__index_keynames,
            "value_keys" : self.__value_keynames,
            "tsstat_filenames" : []
        }
        for key, tsstats in self.items():
            filename = self._get_tsstat_dumpfilename(key)
            fullfilename = os.path.join(outpath, filename)
            if (not os.path.isfile(fullfilename)) or (overwrite is True):
                with open(fullfilename, "wt") as outfile:
                    tsstats.dump(outfile)
            outdata["tsstat_filenames"].append(filename)
        with open(outfilename, "wt") as outfile:
            json.dump(outdata, outfile)

    @staticmethod
    def _filtermatch(key_dict, filterkeys, matchtype):
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
    def _get_load_filenames(path, index_keys, filterkeys=None, matchtype="and"):
        """
        filterkeys could be a part of existing index_keys
        all matching keys will be used
        """
        tsastat_filename = TimeseriesArrayStats.get_dumpfilename(index_keys)
        logging.debug("tsastat_filename: %s", tsastat_filename)
        with open(os.path.join(path, tsastat_filename), "rt") as infile:
            data = json.load(infile)
        logging.debug("loaded json data")
        logging.debug("index_keys: %s", data["index_keys"])
        logging.debug("value_keys: %s", data["value_keys"])
        logging.debug("number of ts files: %s", len(data["tsstat_filenames"]))
        filenames = {}
        for filename in data["tsstat_filenames"]:
            logging.debug("reading key for tsstat from file %s", filename)
            key_enc = filename.split(".")[0][7:] # only this pattern tsstat_(.*).json
            # key_dec = b64decode(key_enc)
            # to not use eval
            # something like: "(u'srvcx221v2.tilak.cc', u'D:\\', u'HOST-RESOURCES-TYPES::hrStorageCompactDisc')" 
            # key = tuple(key_dec.replace("(", "").replace("u'","").replace("', ", " ").replace("')", "").split())
            key = b64eval(key_enc) # must be str not unicode
            key_dict = dict(zip(index_keys, key))
            if filterkeys is not None:
                if TimeseriesArrayStats._filtermatch(key_dict, filterkeys, matchtype):
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
            with open(infilename, "rt") as infile:
                indata = json.load(infile)
        except Exception as exc:
            logging.exception(exc)
            logging.error("something went wrong while loading %s", infilename)
            raise exc
        #logging.info("loaded JSON data: %s", indata)
        tsastats = TimeseriesArrayStats.__new__(TimeseriesArrayStats)
        tsastats.__index_keynames = tuple(indata["index_keys"])
        tsastats.__value_keynames = tuple(indata["value_keys"])
        tsastats.__stats = {}
        tsastats.__autoload = {}
        #for filename in indata["tsstat_filenames"]:
        for key, filename in tsastats._get_load_filenames(path, index_keys, filterkeys, matchtype).items():
            #logging.info("loading TimeseriesStats object from %s", fullfilename)
            #with open(filename, "rt") as infile:
            #    tsastats.__stats[key] = TimeseriesStats.load(infile)
            tsastats.__autoload[key] = filename
            tsastats.__stats[key] = None
        return tsastats

    def __autoload_tsstats(self, filename):
        """
        autoload some stores TimeseriesStats from disk
        """
        with open(filename, "rt") as infile:
            return TimeseriesStats.load(infile)

    def to_data(self):
        """
        full data will be 3 dimensional, so this method returns only structure,
        use get_stats to get 2-dimensional data of specific value_keyname
        """
        ret_data = {
            "index_keynames" : self.__index_keynames,
            "value_keynames" : self.__value_keynames,
            "tsstats_filenames" : [self._get_tsstat_dumpfilename(key) for key in self.keys()],
            "tsastats_filename" : self.get_dumpfilename(self.__index_keynames)
            }
        return ret_data

    def to_json(self):
        """
        full data will be 3 dimensional, so this method returns only structure,
        use get_stats to get 2-dimensional data of specific value_keyname
        """
        ret_data = [
            self.__index_keynames,
            self.__value_keynames,
            [(key, timeseries.stats) for key, timeseries in self.items()]
        ]
        return json.dumps(ret_data)

    @staticmethod
    def from_json(jsondata):
        indata = json.loads(jsondata)
        tsastats = TimeseriesArrayStats.__new__(TimeseriesArrayStats)
        tsastats.__index_keynames = tuple(indata[0])
        tsastats.__value_keynames = tuple(indata[1])
        tsastats.__stats = {}
        for key, tsstats in indata[2]:
            # from json there are only list, but these are not hashable,
            # so convert key to tuple
            tsastats.__stats[tuple(key)] = TimeseriesStats.from_json(json.dumps(tsstats))
        return tsastats

    def to_csv(self, stat_func_name, sortkey=None, reverse=True):
        """
        return csv table of data for one specific statistical function

        first column is always the identifying key of this TimseriesStat as string
        mainly to use in websites to get easier to the key of this row
        """
        yield ("#key", ) + self.__index_keynames + self.__value_keynames
        data = None
        if sortkey is not None:
            data = sorted(self.items(), key=lambda item: item[1][sortkey][stat_func_name], reverse=True)
        else:
            data = self.items()
        for key, value in data:
            values = list(key) + [value[value_key][stat_func_name] for value_key in self.__value_keynames]
            yield (str(key), ) + tuple(values)
