#!/usr/bin/python
# pylint: disable=line-too-long
"""
module to work with TimeseriesArrayStatistics

automatically calculates all TimeseriesStats for every Timeseries in TimeseriesArray
at initialization
"""
import json
import base64
import os
import logging
# own modules
from datalogger.TimeseriesStats import TimeseriesStats as TimeseriesStats
from datalogger.CustomExceptions import TimeseriesEmptyError as TimeseriesEmptyError

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
        self.__index_keynames = tuple([unicode(value) for value in tsa.index_keys])
        self.__value_keynames = tuple([unicode(value) for value in tsa.value_keys])
        for index_key in tsa.keys():
            try:
                self.__stats[index_key] = TimeseriesStats(tsa[index_key])
            except TimeseriesEmptyError as exc:
                logging.info("Timeseries for key %s is length zero, skipping", index_key)

    def __eq__(self, other):
        try:
            assert self.__index_keynames == other.index_keynames
            assert self.__value_keynames == other.value_keynames
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

    def __delitem__(self, key):
        del self.__stats[key]

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
        """DEPRECATED use index_keynames instead"""
        return self.__index_keynames

    @index_keys.setter
    def index_keys(self, value):
        """DEPRECATED use index_keynames instead"""
        self.__index_keynames = value

    @property
    def index_keynames(self):
        return self.__index_keynames

    @index_keynames.setter
    def index_keynames(self, value):
        self.__index_keynames = value

    @property
    def value_keys(self):
        """DEPRECATED use value_keynames instead"""
        return self.__value_keynames

    @value_keys.setter
    def value_keys(self, value):
        """DEPRECATED use value_keynames instead"""
        self.__value_keynames = value

    @property
    def value_keynames(self):
        return self.__value_keynames

    @value_keynames.setter
    def value_keynames(self, value):
        self.__value_keynames = value

#    def group_by_index_keys(self, index_keys):
#        """
#        group tsastat by index_keys, which are a subset of the original index_keys
#
#        the grouping functions are predefined, it makes no sense to make this variable
#
#        parameters:
#        tsastat <TimeseriesArrayStats>
#        index_keys <tuple>
#
#        returns:
#        <TimeseriesArrayStats>
#        """
#        group_funcs = {
#            "sum" : lambda a, b: a + b,
#            "avg" : lambda a, b: (a + b) / 2,
#            "min" : min,
#            "max" : max,
#            "count" : lambda a, b: a + b,
#            "std" : lambda a, b: (a + b) / 2,
#            "median" : lambda a, b: (a + b) / 2,
#            "mean" : lambda a, b: (a + b) / 2,
#            "last" : lambda a, b: (a + b) / 2,
#            "first" : lambda a, b: (a + b) / 2,
#        }
#        try:
#            assert all(index_key in self.__index_keynames for index_key in index_keys)
#        except AssertionError:
#            logging.error("All given index_keys have to be in tsastat.index_keys")
#            return
#        # intermediate data
#        data = {}
#        for key in self.__stats.keys():
#            key_dict = dict(zip(self.__index_keynames, key))
#            group_key = tuple((key_dict[key] for key in index_keys))
#            if group_key not in data:
#                data[group_key] = self.__stats[key].stats
#            else:
#                # there is something to group
#                for value_key in self.__stats[key].keys():
#                    for stat_func, value in self.__stats[key][value_key].items():
#                        # group values by function
#                        grouped_value = group_funcs[stat_func](value, data[group_key][value_key][stat_func])
#                        # store
#                        data[group_key][value_key][stat_func] = grouped_value
#        # get to same format as TimeseriesArrayStats.to_json returns
#        outdata = [self.__index_keynames, self.__value_keynames, ]
#        outdata.append([(key, json.dumps(value)) for key, value in data.items()])
#        # use TimeseriesArrayStats.from_json to get to TimeseriesArrayStats
#        # object
#        new_tsastat = TimeseriesArrayStats.from_json(json.dumps(outdata))
#        return new_tsastat

    def slice(self, value_keys):
        """
        remove all values_keys not in value_keys, and return new TimeseriesArrayStats object
        """
        assert all((value_key in self.__value_keynames for value_key in value_keys))
        outdata = []
        outdata.append(self.__index_keynames)
        outdata.append(value_keys)
        tsstat_data = []
        for key, tsstat in self.__stats.items():
            data = {}
            for value_key in value_keys:
                data[value_key] = tsstat[value_key]
            tsstat_data.append((key, json.dumps(data)))
        outdata.append(tsstat_data)
        new_tsastat = TimeseriesArrayStats.from_json(json.dumps(outdata))
        return new_tsastat

    def get_stats(self, value_key):
        """
        returns dictionary of stats of every Timeseries object in Array for this
        specific value_key only

        parameters:
        value_key <str> must be in self.value_keys

        returns:
        <dict>
        """
        assert value_key in self.__value_keynames
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
        #logging.info("index_keys: %s", self.__index_keynames)
        outfilename = os.path.join(outpath, self.get_dumpfilename(self.__index_keynames))
        outdata = {
            "index_keys" : self.__index_keynames,
            "value_keys" : self.__value_keynames,
            "tsstat_filenames" : []
        }
        for key, tsstats in self.__stats.items():
            filename = self.get_tsstat_dumpfilename(key)
            fullfilename = os.path.join(outpath, filename)
            if (not os.path.isfile(fullfilename)) or (overwrite is True):
                filehandle = open(fullfilename, "wb")
                tsstats.dump(filehandle)
                filehandle.flush()
            outdata["tsstat_filenames"].append(filename)
        filehandle = open(outfilename, "wb")
        json.dump(outdata, filehandle)
        filehandle.flush()

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
            fh = open(infilename, "rb")
            indata = json.load(fh)
        except Exception as exc:
            logging.exception(exc)
            logging.error("something went wrong while loading %s", infilename)
            raise exc
        #logging.info("loaded JSON data: %s", indata)
        tsastats = TimeseriesArrayStats.__new__(TimeseriesArrayStats)
        tsastats.__index_keynames = tuple(indata["index_keys"])
        tsastats.__value_keynames = tuple(indata["value_keys"])
        tsastats.__stats = {}
        #for filename in indata["tsstat_filenames"]:
        for key, filename in tsastats.get_load_filenames(path, index_keys, filterkeys, matchtype).items():
            #logging.info("loading TimeseriesStats object from %s", fullfilename)
            filehandle = open(filename, "rb")
            tsastats.__stats[key] = TimeseriesStats.load(filehandle)
            filehandle.close()
        return tsastats

    def to_json(self):
        outdata = []
        outdata.append(self.__index_keynames)
        outdata.append(self.__value_keynames)
        outdata.append([(key, tsstat.to_json()) for key, tsstat in self.__stats.items()])
        try:
            return json.dumps(outdata)
        except TypeError as exc:
            logging.exception(exc)
            logging.error(outdata)
            raise exc

    @staticmethod
    def from_json(jsondata):
        indata = json.loads(jsondata)
        tsastats = TimeseriesArrayStats.__new__(TimeseriesArrayStats)
        tsastats.__index_keynames = indata[0]
        tsastats.__value_keynames = indata[1]
        tsastats.__stats = {}
        for key, tsstats in indata[2]:
            # from json there are only list, but these are not hashable,
            # so convert key to tuple
            tsastats.__stats[tuple(key)] = TimeseriesStats.from_json(tsstats)
        return tsastats

    def remove_by_value(self, value_key, stat_func_name, value):
        """
        remove key from internal data, if condition is met
        """
        for key, tsstats in self.__stats.items():
            if tsstats[value_key][stat_func_name] == value:
                del self.__stats[key]

    def to_csv(self, stat_func_name, sortkey=None, reverse=True):
        """
        return csv table of data for one specific statistical function

        first column is always the identifying key of this TimseriesStat as string
        mainly to use in websites to get easier to the key of this row
        """
        outbuffer = []
        outbuffer.append((u"#key", ) + self.__index_keynames + self.__value_keynames)
        data = None
        if sortkey is not None:
            data = sorted(self.__stats.items(), key=lambda item: item[1][sortkey][stat_func_name], reverse=True)
        else:
            data = self.__stats.items()
        for key, value in data:
            values = list(key) + [value[value_key][stat_func_name] for value_key in self.__value_keynames]
            outbuffer.append([unicode(key), ] + values)
        return outbuffer


