#!/usr/bin/python
# pylint: disable=line-too-long
"""
module for TiemseriesArray Class
"""
import sys
import re
import logging
import base64
import json
import os
import gzip
# own modules
from Timeseries import Timeseries as Timeseries
from TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats


#################### hack begin ##########################
"""
hack to mimic some python 2.x behaviour is string
representation of tuples
"""
def _b64encode_p3(list_obj):
    if len(list_obj) == 1:
        start ="(u'" + list_obj[0] + "',)"
    else:
        start ="(u'" + "', u'".join((str(key) for key in list_obj)) + "')"
    encoded = base64.urlsafe_b64encode(start.encode("utf-8")).decode("utf-8")
    #print("%s -> %s -> %s" % (list_obj, encoded, b64decode(encoded)))
    return encoded

def _b64encode_p2(list_obj):
    encoded = base64.urlsafe_b64encode(unicode(tuple(list_obj))).decode("utf-8")
    #print("%s -> %s -> %s" % (list_obj, encoded, b64decode(encoded)))
    return encoded

def _b64decode(encoded):
    decoded = base64.b64decode(encoded).decode("utf-8")
    #print("%s -> %s" % (encoded, decoded))
    return decoded


if sys.version_info < (3,0):
    print("using python 2 coding funtions")
    b64encode = _b64encode_p3
    b64decode = _b64decode
else:
    b64encode = _b64encode_p3
    b64decode = _b64decode
##################### hack end ###########################

def is_near(value, target_value, pct=0.05):
    """
    function to implement if some numeric is near, in terms of percent
    handy to use with floating point equality
    """
    minimum = float(target_value) * (1.0 - pct)
    maximum = float(target_value) * (1.0 + pct)
    return minimum < float(value) < maximum


class TimeseriesArray(object):
    """
    holds dictionary of Timeseries objects
    """
    group_funcs = {
        "sum" : sum,
        "min" : min,
        "max" : max,
        "avg" : lambda a: sum(a) / len(a),
        "len" : len,
    }

    def __init__(self, index_keynames, value_keynames, ts_key="ts", datatypes=None, cache=False):
        """
        index_keys <tuple> column names of index columns
        value_keys <tuple> column names of value columns
        ts_key <str> name of timestamp column
        datatypes <list> list of used datatypes
        cache <bool> should already loaded timeseries be cached, useful to calculate quantiles
        """
        self.__index_keynames = tuple([value for value in index_keynames])
        self.__value_keynames = list([value for value in value_keynames])
        self.__ts_key = ts_key
        self.__cache = cache
        # define instance data
        self.__debug = False
        self.__data = {} # holds data
        self.ts_autoload = {} # holds key to ts filename dict
        self.datatypes = datatypes
        self.__group_keyname = None # for future use
        self.__group_func = None # for future use

    def __len__(self):
        """mimic dict"""
        return len(self.__data.keys())

    def __str__(self):
        """
        return string represenation for tsa,
        mainly the same as in stored version
        """
        outbuffer = {
            "index_keys" : self.__index_keynames,
            "value_keys" : self.__value_keynames,
            "ts_key" : self.__ts_key,
            "ts_filenames" : [self.get_ts_dumpfilename(key) for key in self.keys()],
            "tsa_filename" : self.get_dumpfilename(self.__index_keynames),
            "datatypes" : self.datatypes
        }
        return json.dumps(outbuffer, indent=4, sort_keys=True)

    def __getitem__(self, key):
        """mimic dict, honor lazy reloading of Timeseries if value is None"""
        if self.__data[key] is None:
            # auto load data if None
            timeseries = self.__autoload_ts(key)
            if self.__cache is False:
                # if cache is set to None, return only loaded data,
                # but do not save reference, so every later call will
                # result in re-read of timeseries
                return timeseries
            self.__data[key] = timeseries
        return self.__data[key]

    def __setitem__(self, key, value):
        """mimic dict"""
        self.__data[key] = value

    def __delitem__(self, key):
        """mimic dict"""
        del self.__data[key]

    def __eq__(self, other):
        """test equality in depth"""
        try:
            assert self.__index_keynames == other.index_keynames
            assert self.__value_keynames == other.value_keynames
            assert self.__ts_key == other.ts_key
            assert len(self.__data.keys()) == len(other.keys())
            for key in self.__data.keys():
                if len(self[key]) != len(other[key]):
                    raise AssertionError("timeseries %s data differences in length" % key)
            return True
        except AssertionError as exc:
            logging.exception(exc)
            return False

    def items(self):
        """mimic dict, but honor autoload feature"""
        for key in self.keys():
            yield (key, self[key])

    def values(self):
        """mimic dict, but honor autoload feature"""
        for key in self.__data.keys():
            yield self[key]

    def keys(self):
        """
        mimic dict behaviour
        """
        return self.__data.keys()

    def get_index_dict(self, index_values):
        """
        index_values <tuple> as used in self.data.keys()

        return <dict> representation of given index_values
        """
        return dict(zip(self.__index_keynames, index_values))

    @property
    def index_keynames(self):
        """keynames which build key for self.data"""
        return self.__index_keynames

    @property
    def value_keynames(self):
        """keynames which build value fields for Timeseries objects"""
        return self.__value_keynames

    @property
    def ts_key(self):
        """keyname of timestamp"""
        return self.__ts_key

    @property
    def stats(self):
        """return TimeseriesArrayStats from self"""
        return TimeseriesArrayStats(self)

    @property
    def debug(self):
        """set some debugging on or off"""
        return self.__debug

    @debug.setter
    def debug(self, value):
        """set this to True to get more debug messages, like raw data value errors"""
        assert isinstance(value, bool)
        self.__debug = value

    @property
    def cache(self):
        """True if timeseries will be cached in memory"""
        return self.__cache

    @cache.setter
    def cache(self, value):
        """set to True if every loaded timeseries should be cached in memory"""
        assert isinstance(value, bool)
        self.__cache = value

    def set_group_keyname(self, index_keyname, group_func):
        """
        set index_keyname to group values for

        index_keyname <basestring> in self.index_keynames
        group_func <func> function to group multiple values by

        if set every __getitem__ call will group data automatically
        """
        assert index_keyname in self.__index_keynames
        self.__group_keyname = index_keyname
        self.__group_func = group_func

    @staticmethod
    def to_float(value_str):
        """
        try to convert string to float, honor "," als decimal point if possible
        otherwise raise ValueError
        """
        try:
            return float(value_str) # first best
        except ValueError:
            return float(value_str.replace(u",", u".")) # try to replace colon with point

    def add(self, data, group_func=None):
        """
        provided data must be type <dict> having following keys
            ts_keyname
            all(index_keynames)
            all(value_keynames)
        if there are additional keys, these will be ignored

        if group_func is given, entries with the same index_key are grouped by group_func
        group func should be type <func> something like
            lambda existing_value, new_value : (existing_value + new_value) / 2

        all index_keys are converted to str
        all value_keys are converted to float
        ts_keyname is converted to float
        """
        #assert self.__ts_key in data # timestamp key has to be in dict
        #assert (type(data[self.__ts_key]) == int) or (type(data[self.__ts_key]) == float) # timestamp should be int
        #assert all((value_key in data for value_key in self.__value_keynames)) # test if all keys are available
        # create key from data
        try:
            index_key = tuple([data[key] for key in self.__index_keynames])
        except KeyError:
            #logging.exception(exc)
            logging.error("there are index_keys missing in this dataset %s, skipping this dataset", data.keys())
            return
        # add data to this timeseries object
        try:
            # timestamp and values has to be converted to float
            # made the next explicit to avoid empty keys if there is no
            # valueable data -> will result in ValueError
            ts = float(data[self.__ts_key])
            values = [self.to_float(data[key]) for key in self.__value_keynames] # must be list not tuple, to be added to another list
            if index_key not in self.keys():
                # if this key is new, create empty Timeseries object
                logging.debug("first entry for index_key : %s", index_key)
                self[index_key] = Timeseries(self.__value_keynames)
            if group_func is not None:
                self[index_key].group_add(ts, values, group_func)
            else:
                self[index_key].add(ts, values)
        except KeyError as exc:
            #logging.exception(exc)
            if self.__debug: # some datasources have incorrect data
                logging.error(exc)
                logging.error("there is some key missing in %s, should be %s and %s, skipping this dataset, skipping this dataset", data.keys(), self.__ts_key, self.__value_keynames)
        except ValueError as exc:
            #logging.exception(exc)
            if self.__debug: # some datasources have incorrect data
                logging.error(exc)
                logging.error("some value_keys or ts_keyname are not numeric and float convertible, skipping this dataset: %s", data)

    def group_add(self, data, group_func):
        """wrapper to be api consistent, DEPRECATED"""
        return self.add(data, group_func)

    def append(self, key, timeserie):
        """
        key <str> key to store this timeseries in dict
        timeserie <Timeseries> object that holds the data

        append whole timeserie to existing data
        data length must be the same, but start_ts and stop_ts can be slightly different for each key
        """
        #logging.debug("new start : %s, stop: %s, length %s", timeserie[0][0], timeserie[-1][0], len(timeserie))
        assert key not in self.keys()
        if len(self.keys()) > 0:
            #logging.debug("existing start : %s, stop: %s, length %s", self.data.values()[0][0][0], self.data.values()[0][-1][0], len(self.data.values()[0]))
            try:
                assert len(timeserie) == len(self.values()[0]) # must be the same length
            except AssertionError:
                logging.error("The Timeseries Object to append, has not the same length as existing data in this array")
                logging.error("existing: %d, new %d", len(timeserie), len(self.values()[0]))
        else:
            logging.debug("this is the first timeseries")
        self[key] = timeserie

    def old_groupby(self, fieldnum, group_func, time_func="avg"):
        """
        fieldnum <int>
        group_func <function> to group a set of numeric values
        time_func

        aggregate data on one of the index keys, example

        <ts1> <hostname> <intance0> <value1>
        <ts1> <hostname> <intance1> <value2>
        <ts1> <hostname> <intance2> <value3>
        <ts2> <hostname> <intance0> <value1> # here timeseries 2 starts
        <ts2> <hostname> <intance1> <value2>
        <ts2> <hostname> <intance2> <value3>

        to

        <hostname> group_func(time_func(value1) + time_func(value2) + time_func(value3))
        """
        ret_data = {}
        for key in self.keys():
            subkey = key[fieldnum]
            try:
                stat_data = self[key].get_stat(time_func)
                if subkey not in ret_data:
                    ret_data[subkey] = stat_data
                    ret_data[subkey]["count"] = 1
                else:
                    #aggregate, there are more than one row who match
                    for valuekey in stat_data:
                        try:
                            ret_data[subkey][valuekey] = group_func(ret_data[subkey][valuekey], stat_data[valuekey])
                        except TypeError as exc:
                            logging.exception(exc)
                            logging.error("error at operation group_func(%s, %s)", ret_data[subkey][valuekey], stat_data[valuekey])
                    ret_data[subkey]["count"] += 1
            except Exception as exc:
                logging.exception(exc)
        return ret_data

    def convert(self, colname, datatype, newcolname=None):
        """
        call convert method of every stored Timeseries, with given parameter
        """
        if self.__cache is False:
            raise AttributeError("operation only applicable in cache mode, set <TimeseriesArray>.cache=True")
        if colname not in self.__value_keynames:
            raise KeyError("colname %s not in defined columns" % colname)
        if newcolname in self.__value_keynames:
            raise KeyError("newcolname %s already in defined columns" % newcolname)
        for key in self.keys():
            self[key].convert(colname, datatype, newcolname)
        self.__value_keynames.append(newcolname)

    def add_derive_col(self, colname, newcolname):
        logging.info("DEPRECATED function add_derive_col use convert(%s, 'derive', %s)", colname, newcolname)
        return self.convert(colname, "derive", newcolname)

    def add_per_s_col(self, colname, newcolname):
        logging.info("DEPRECATED function add_derive_col use convert(%s, 'persecond', %s)", colname, newcolname)
        return self.convert(colname, "persecond", newcolname)

    def add_calc_col_single(self, colname, newcolname, func=lambda a: a):
        """
        add a new column to every Timeserie, calculated from one single column in this row

        parameters:
        colname <str> name of existing column
        newcolname <str> name of new column
        func <func> function to call for every row, wtih data from colname
            lambda <float> : <float>

        return:
        None
        """
        if self.__cache is False:
            raise AttributeError("operation only applicable in cache mode, set <TimeseriesArray>.cache=True")
        if colname not in self.__value_keynames:
            raise KeyError("colname %s not in defined columns" % colname)
        if newcolname in self.__value_keynames:
            raise KeyError("newcolname %s already in defined columns" % newcolname)
        for key in self.keys():
            self[key].add_calc_col_single(colname, newcolname, func)
        self.__value_keynames.append(newcolname)

    def add_calc_col_full(self, newcolname, func):
        """
        add a new column to every Timeserie, calculated fro existing row data

        parameters:
        newcolname <str> name of new column
        func <func> function to call for every existing row
            lambda <dict> : <float>

        returns:
        None
        """
        if self.__cache is False:
            raise AttributeError("operation only applicable in cache mode, set <TimeseriesArray>.cache=True")
        if newcolname in self.__value_keynames:
            raise KeyError("newcolname %s already in defined columns" % newcolname)
        for key in self.keys():
            self[key].add_calc_col_full(newcolname, func)
        self.__value_keynames.append(newcolname)

    def remove_col(self, colname):
        """
        remove column named from every Timeserie

        parameters:
        colname <str>

        returns:
        None
        """
        if self.__cache is False:
            raise AttributeError("operation only applicable in cache mode, set <TimeseriesArray>.cache=True")
        if colname not in self.__value_keynames:
            raise KeyError("colname %s not in defined columns" % colname)
        for key in self.keys():
            self[key].remove_col(colname)
        self.__value_keynames.remove(colname)

    def slice(self, colnames):
        """
        return copy of TimeseriesArray, but only defined value_keynames

        parameters:
        colnames <list> of value_keynames in returned TimeseriesArray

        returns:
        TimeseriesArray
        """
        ret_data = TimeseriesArray(index_keynames=self.__index_keynames, value_keynames=colnames, ts_key=self.__ts_key)
        for key in self.keys():
            ret_data[key] = self[key].slice(colnames)
        return ret_data

    def export(self):
        """
        function to export all stored data in such a manner, that this is easily used to feed the add function of another TimeseriesArray object

        create new object tsa_new (tsa is the existing one) like

        tsa_new = TimeseriesArray(tsa.index_keys, tsa.value_keys, tsa.ts_key)

        and you can map(tsa.add(), tsa.export())
        """
        for key in self.keys():
            timeseries = self[key]
            # convert key tuple to dict
            key_dict = self.get_index_dict(key)
            # dump timeseries as dictionary and spice dict up with
            # key_dict
            for row in self[key].to_dict():
                row.update(key_dict)
                yield row

    def dump(self, outpath, overwrite=False):
        """
        dump all data to directory in csv format, filename will be auto generated

        parameters:
        outpath <str> must be existing directory
        overwrite <bool> overwrite existing Timeseries files, or not
            the TimeseriesArray file is witten nonetheless if this options is set or not
        """
        tsa_filename = self.get_dumpfilename(self.__index_keynames)
        logging.debug("tsa_filename: %s", tsa_filename)
        tsa_outfilename = os.path.join(outpath, tsa_filename)
        outbuffer = {
            "index_keys" : self.__index_keynames,
            "value_keys" : self.__value_keynames,
            "ts_key" : self.__ts_key,
            "ts_filenames" : []
        }
        for key in self.keys():
            timeseries = self[key]
            ts_filename = self.get_ts_dumpfilename(key)
            # skip dump, if file exists, and overwrite=False
            ts_outfilename = os.path.join(outpath, ts_filename)
            if not os.path.isfile(ts_outfilename) or overwrite:
                logging.debug("dumping key %s to filename %s", key, ts_filename)
                with gzip.open(ts_outfilename, "wt") as outfile:
                    timeseries.dump(outfile)
            outbuffer["ts_filenames"].append(ts_filename)
        with open(tsa_outfilename, "wt") as outfile:
            json.dump(outbuffer, outfile)
            outfile.flush()
    dump_split = dump

    @staticmethod
    def get_ts_dumpfilename(key):
        """
        return filename of Timeseries dump File

        parameters:
        key <tuple> index_key of this particular Timeseries

        returns:
        <str>
        """
        return "ts_%s.csv.gz" % b64encode(key)

    @staticmethod
    def get_dumpfilename(index_keys):
        """
        return filename of TimseriesArray dump file

        parameters:
        index_keys <tuple> of particular index_keys of this Timeseries

        returns:
        <str>
        """
        return "tsa_%s.json" % b64encode(index_keys)

    @staticmethod
    def filtermatch(key_dict, filterkeys, matchtype):
        """
        key_dict is the whole index key, aka
        {hostname : test, instance:1, other:2}

        filterkey is part
        {hostname : test}
        {hostname : test, instance: None, other: None}

        TODO: to be improved
        """
        assert matchtype in ("and", "or")
        matched = 0
        for key in filterkeys.keys():
            if filterkeys[key] is None: # ignore keys with value None
                if matchtype == "and": # and count them as matched
                    matched += 1
                continue
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
    def get_ts_filenames(path, index_keys, filterkeys=None, matchtype="and"):
        """
        filterkeys could be a part of existing index_keys
        all matching keys will be used
        """
        tsa_filename = TimeseriesArray.get_dumpfilename(index_keys)
        logging.debug("tsa_filename: %s", tsa_filename)
        with open(os.path.join(path, tsa_filename), "rt") as infile:
            data = json.load(infile)
        logging.debug("loaded json data")
        logging.debug("index_keys: %s", data["index_keys"])
        logging.debug("value_keys: %s", data["value_keys"])
        logging.debug("ts_key: %s", data["ts_key"])
        logging.debug("number of ts files: %s", len(data["ts_filenames"]))
        filenames = {}
        for filename in data["ts_filenames"]:
            logging.debug("parsing Timeseries filename %s", filename)
            enc_key = filename.split(".")[0][3:] # only this pattern ts_(.*).csv.gz
            key = eval(b64decode(enc_key))
            key_dict = dict(zip(index_keys, key))
            if filterkeys is not None:
                if TimeseriesArray.filtermatch(key_dict, filterkeys, matchtype):
                    logging.debug("adding tsa key : %s", key)
                    filenames[key] = os.path.join(path, filename)
            else:
                # no filterkeys means every file is loaded
                logging.debug("adding tsa key : %s", key)
                filenames[key] = os.path.join(path, filename)
        return filenames

    @staticmethod
    def load(path, index_keys, filterkeys=None, index_pattern=None, matchtype="and", datatypes=None):
        """
        load stored tsa data from directory <path>

        filterkeys could be a part of existing index_keys
        all matching keys will be used

        index_keys <tuple> * required
        filterkeys <tuple> default None
        matchtype <str> default "and"
        index_pattern <str> for use in re.compile(index_pattern)

        return:
        <TimeseriesArray>
        """
        # get filename and load json structure
        tsa_filename = TimeseriesArray.get_dumpfilename(index_keys)
        with open(os.path.join(path, tsa_filename), "rt") as infile:
            data = json.load(infile)
        # create object
        tsa = TimeseriesArray(data["index_keys"], data["value_keys"], data["ts_key"], datatypes=datatypes)
        # load full or filter some keys
        if index_pattern is None:
            for key, filename in tsa.get_ts_filenames(path, index_keys, filterkeys, matchtype).items():
                tsa.ts_autoload[key] = filename
                tsa[key] = None
        else:
            logging.info("using index_pattern %s to filter index_keys", index_pattern)
            rex = re.compile(index_pattern)
            for key, filename in tsa.get_ts_filenames(path, index_keys, filterkeys, matchtype).items():
                m = rex.match(str(key))
                if m is not None:
                    tsa.ts_autoload[key] = filename
                    tsa[key] = None
                else:
                    logging.debug("index_key %s skipped by filter", key)
        return tsa
    load_split = load

    def __autoload_ts(self, key):
        """
        try to load TimeSeries given by key
        key has to be already in TimeseriesArray structure

        parameters:
        key : <tuple>
        """
        if key in self.ts_autoload:
            filename = self.ts_autoload[key]
            logging.debug("auto-loading Timeseries from file %s", filename)
            with gzip.open(filename, "rt") as infile:
                timeseries = Timeseries.load_from_csv(infile)
            # convert raw timeseries to datatype
            for colname, datatype in self.datatypes.items():
                if datatype == "asis":
                    continue
                timeseries.convert(colname, datatype, None)
            return timeseries
        else:
            raise KeyError("key %s not in TimeseriesArray", key)

TimeseriesArrayLazy = TimeseriesArray
