#!/usr/bin/python
# pylint: disable=line-too-long
"""
module for TiemseriesArray Class
"""
import re
import logging
import base64
import json
import os
import gzip
# own modules
from Timeseries import Timeseries as Timeseries
from TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats


def is_near(value, target_value, pct=0.05):
    """
    function to implement if some numeric is near, in terms of percent
    handy to use with floating point equality
    """
    minimum = float(target_value) * (1.0 - pct)
    maximum = float(target_value) * (1.0 + pct)
    return minimum < float(value) < maximum


class TimeseriesArrayLazy(object):
    """
    holds dictionary of Timeseries objects
    """
    group_funcs = {
        "sum" : lambda a: sum(a),
        "min" : lambda a: min(a),
        "max" : lambda a: max(a),
        "avg" : lambda a: sum(a) / len(a),
        "len" : lambda a: len(a),
    }

    def __init__(self, index_keys, value_keys, ts_key="ts", datatypes=None):
        """
        index_keys <tuple> column names of index columns
        value_keys <tuple> column names of value columns
        ts_key <str> name of timestamp column
        """
        self.__index_keys = tuple([unicode(value) for value in index_keys])
        self.__value_keys = list([unicode(value) for value in value_keys])
        self.__ts_key = unicode(ts_key)
        # define instance data
        self.__debug = False
        self.__data = {} # holds data
        self.ts_autoload = {} # holds key to ts filename dict
        self.datatypes = datatypes

    def __len__(self):
        """mimic dict"""
        return len(self.__data.keys())

    def __getitem__(self, key):
        """mimic dict, honor lazy reloading of Timeseries"""
        return self.__autoload_ts(key)

    def __setitem__(self, key, value):
        """mimic dict"""
        self.__data[key] = value

    def __delitem__(self, key):
        """mimic dict"""
        del self.__data[key]

    def __eq__(self, other):
        """test equality in depth"""
        try:
            assert self.__index_keys == other.index_keys
            assert self.__value_keys == other.value_keys
            assert self.__ts_key == other.ts_key
            assert len(self.__data.keys()) == len(other.keys())
            for key in self.__data.keys():
                if len(self.__data[key]) != len(other.data[key]):
                    raise AssertionError("timeseries %s data differences in length" % key)
            return True
        except AssertionError as exc:
            logging.exception(exc)
            return False

    def items(self):
        """mimic dict"""
        return ((key, self[key]) for key in self.keys())

    def values(self):
        """mimic dict"""
        for key in self.__data.keys():
            yield self[key]

    def keys(self):
        """
        return keys of internal data object
        mimic dict behaviour
        """
        return self.__data.keys()

    def get_index_dict(self, index_values):
        """
        index_values <tuple> as used in self.data.keys()

        return <dict> representation of given index_values
        """
        return dict(zip(self.__index_keys, index_values))

#    @property
#    def data(self):
#        """dictionary to hold Timeseries objects"""
#        return self.__data

    @property
    def index_keys(self):
        """keynames which build key for self.data"""
        return self.__index_keys

    @property
    def value_keys(self):
        """keynames which build value fields for Timeseries objects"""
        return self.__value_keys

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

    def add(self, data, group_func=None):
        """
        data must have following keys
        <ts_keyname> <index_keys> <value_keys>
        if there are additional keys, these will be ignored

        All index_keys are converted to unicode
        All value_keys are converted to float
        ts_keyname is converted to float
        """
        #assert self.__ts_key in data # timestamp key has to be in dict
        #assert (type(data[self.__ts_key]) == int) or (type(data[self.__ts_key]) == float) # timestamp should be int
        #assert all((value_key in data for value_key in self.__value_keys)) # test if all keys are available
        # create key from data
        try:
            key = tuple([unicode(data[key]) for key in self.__index_keys])
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
            values = tuple((float(data[key]) for key in self.__value_keys))
            if key not in self.keys():
                # if this key is new, create empty Timeseries object
                self[key] = Timeseries(self.__value_keys)
            if group_func is not None:
                self[key].group_add(ts, values, group_func)
            else:
                self[key].add(ts, values)
        except KeyError as exc:
            #logging.exception(exc)
            if self.__debug: # some datasources have incorrect data
                logging.error("there is some key missing in %s, should be %s and %s, skipping this dataset, skipping this dataset", data.keys(), self.__ts_key, self.__value_keys)
        except ValueError as exc:
            #logging.exception(exc)
            if self.__debug: # some datasources have incorrect data
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

    def groupby(self, fieldnum, group_func, time_func="avg"):
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
            except StandardError as exc:
                logging.exception(exc)
        return ret_data

    def convert(self, colname, datatype, newcolname=None):
        """
        call convert method of every stored Timeseries, with given parameter
        """
        for key in self.keys():
            timeseries = self[key]
            timeseries.convert(colname, datatype, newcolname)

    def add_derive_col(self, colname, newcolname):
        """
        add one to key to every Timeseries, for this specific colname
        which represents the difference between two values in time
        TODO: describe this better
        """
        assert colname in self.__value_keys
        assert newcolname not in self.__value_keys
        for key, timeseries in self.keys():
            timeseries = self[key]
            try:
                assert newcolname not in timeseries.headers
            except AssertionError as exc:
                logging.error("%s does exist in current dataset at key %s", newcolname, key)
                raise exc
            timeseries.add_derive_col(colname, newcolname)
        self.__value_keys.append(newcolname)

    def add_per_s_col(self, colname, newcolname):
        """
        add one to key to every Timeseries, for this specific colname
        which represents the difference between two values in time
        TODO: describe this better
        """
        assert colname in self.__value_keys
        assert newcolname not in self.__value_keys
        for timeseries in self.values():
            assert newcolname not in timeseries.headers
            timeseries.add_per_s_col(colname, newcolname)
        self.__value_keys.append(newcolname)

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
        assert colname in self.__value_keys
        assert newcolname not in self.__value_keys
        for timeseries in self.values():
            assert newcolname not in timeseries.headers
            timeseries.add_calc_col_single(colname, newcolname, func)
        self.__value_keys.append(newcolname)

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
        assert newcolname not in self.__value_keys
        for timeseries in self.values():
            assert newcolname not in timeseries.headers
            timeseries.add_calc_col_full(newcolname, func)
        self.__value_keys.append(newcolname)

    def remove_col(self, colname):
        """
        remove column named from every Timeserie

        parameters:
        colname <str>

        returns:
        None
        """
        assert colname in self.__value_keys
        for key in self.keys():
            timeseries = self[key]
            try:
                assert colname in timeseries.headers
            except AssertionError as exc:
                logging.error("Timeseries with key %s, has no header %s, actually only %s", key, colname, timeseries.headers)
                raise exc
            timeseries.remove_col(colname)
        self.__value_keys.remove(colname)

    def slice(self, colnames):
        ret_data = TimeseriesArray(index_keys=self.__index_keys, value_keys=colnames, ts_key=self.__ts_key)
        for key in self.keys():
            timeseries = self[key]
            ret_data.data[key] = value.slice(colnames)
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
            # key_dict and timestamp
            ts_dump_dict = timeseries.dump_dict()
            for timestamp in sorted(ts_dump_dict.keys()):
                row = ts_dump_dict[timestamp]
                row.update(key_dict)
                row[self.ts_key] = timestamp
                yield row

    def dump(self, outpath, overwrite=False):
        """
        dump all data to filehandle in csv format
        filehandle can also be gzip.open or other types

        parameters:
        filehandle <file>
        overwrite <bool> overwrite existing Timeseries files, or not
            the json file is witten nonetheless if this options is set or not
        """
        tsa_filename = self.get_dumpfilename(self.__index_keys)
        logging.debug("tsa_filename: %s", tsa_filename)
        outfile = os.path.join(outpath, tsa_filename)
        outbuffer = {
            "index_keys" : self.__index_keys,
            "value_keys" : self.__value_keys,
            "ts_key" : self.__ts_key,
            "ts_filenames" : []
        }
        for key in self.keys():
            timeseries = self[key]
            ts_filename = self.get_ts_dumpfilename(key)
            # skip dump, if file exists, and overwrite=False
            if not os.path.isfile(os.path.join(outpath, ts_filename)) or overwrite:
                logging.debug("dumping key %s to filename %s", key, ts_filename)
                ts_filehandle = gzip.open(os.path.join(outpath, ts_filename), "wb")
                timeseries.dump_to_csv(ts_filehandle)
                ts_filehandle.close() # close to not get too many open files
            outbuffer["ts_filenames"].append(ts_filename)
        json.dump(outbuffer, open(outfile, "wb"))
    dump_split = dump

    @staticmethod
    def get_ts_dumpfilename(key):
        return "ts_%s.csv.gz" % base64.urlsafe_b64encode(unicode(key))

    @staticmethod
    def get_dumpfilename(index_keys):
        return "tsa_%s.json" % base64.urlsafe_b64encode(unicode(index_keys))

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
        tsa_filename = TimeseriesArrayLazy.get_dumpfilename(index_keys)
        logging.debug("tsa_filename: %s", tsa_filename)
        infile = os.path.join(path, tsa_filename)
        filehandle = open(infile, "rb")
        data = json.load(filehandle)
        filehandle.close()
        logging.debug("loaded json data")
        logging.debug("index_keys: %s", data["index_keys"])
        logging.debug("value_keys: %s", data["value_keys"])
        logging.debug("ts_key: %s", data["ts_key"])
        logging.debug("number of ts files: %s", len(data["ts_filenames"]))
        filenames = {}
        for filename in data["ts_filenames"]:
            logging.debug("reading ts from file %s", filename)
            enc_key = filename.split(".")[0][3:] # only this pattern ts_(.*).csv.gz
            key = eval(base64.urlsafe_b64decode(str(enc_key))) # must be str not unicode
            key_dict = dict(zip(index_keys, key))
            if filterkeys is not None:
                if TimeseriesArrayLazy.filtermatch(key_dict, filterkeys, matchtype):
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
        filterkeys could be a part of existing index_keys
        all matching keys will be used

        index_keys <tuple>
        filterkeys <tuple> default None
        matchtype <str> default "and"
        index_pattern <str> for use in re.compile(index_pattern)

        return:
        <TimeseriesArray>
        """
        # get filename and load json structure
        tsa_filename = TimeseriesArrayLazy.get_dumpfilename(index_keys)
        infile = os.path.join(path, tsa_filename)
        try:
            filehandle = open(infile, "rb")
            data = json.load(filehandle)
            filehandle.close()
        except StandardError as exc:
            logging.exception(exc)
            logging.error("Something went wrong while loading json data from %s", tsa_filename)
            raise exc
        # create object
        tsa = TimeseriesArrayLazy(data["index_keys"], data["value_keys"], data["ts_key"], datatypes=datatypes)
        if index_pattern is None:
            for key, filename in tsa.get_ts_filenames(path, index_keys, filterkeys, matchtype).items():
                tsa.ts_autoload[key] = filename
                tsa[key] = None
                #logging.debug("loading Timeseries from file %s", filename)
                #filehandle = gzip.open(filename, "rb")
                #tsa.data[key] = Timeseries.load_from_csv(filehandle)
                #filehandle.close() # close to not get too many open files
        else:
            logging.info("using index_pattern %s to filter loaded keys", index_pattern)
            rex = re.compile(index_pattern)
            for key, filename in tsa.get_ts_filenames(path, index_keys, filterkeys, matchtype).items():
                m = rex.match(unicode(key))
                if m is not None:
                    tsa.ts_autoload[key] = filename
                    tsa[key] = None
                    #logging.debug("loading Timeseries from file %s", filename)
                    #filehandle = gzip.open(filename, "rb")
                    #tsa.data[key] = Timeseries.load_from_csv(filehandle)
                    #filehandle.close() # close to not get too many open files
                else:
                    logging.info("index_key %s filtered", key)
        return tsa
    load_split = load

    def __autoload_ts(self, key):
        """
        try to load TimeSeries given by key
        key has to be already in TimeseriesArray structure
        """
        if key in self.ts_autoload:
            filename = self.ts_autoload[key]
            logging.debug("auto-loading Timeseries from file %s", filename)
            filehandle = gzip.open(filename, "rb")
            timeseries = Timeseries.load_from_csv(filehandle)
            filehandle.close() # close to not get too many open files
            # convert raw timeseries to datatype
            for colname, datatype in self.datatypes.items():
                if datatype == "asis":
                    continue
                timeseries.convert(colname, datatype, None)
            return timeseries
        else:
            raise KeyError("key %s not in TimeseriesArray", key)
