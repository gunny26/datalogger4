#!/usr/bin/python
# pylint: disable=line-too-long
"""
module for TiemseriesArray Class
"""
import logging
import base64
import json
import os
import gzip
import numpy
# own modules
from Timeseries import Timeseries as Timeseries


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
        "sum" : lambda a: sum(a),
        "min" : lambda a: min(a),
        "max" : lambda a: max(a),
        "avg" : lambda a: sum(a) / len(a),
        "len" : lambda a: len(a),
    }

    def __init__(self, index_keys, value_keys, ts_key="ts"):
        """
        index_keys <tuple> column names of index columns
        value_keys <tuple> column names of value columns
        ts_key <str> name of timestamp column
        """
        self.__index_keys = tuple([unicode(value) for value in index_keys])
        self.__value_keys = list([unicode(value) for value in value_keys])
        self.__ts_key = unicode(ts_key)
        # define instance data
        self.__data = {} # holds data

    def __len__(self):
        """mimic dict"""
        return len(self.__data.keys())

    def __getitem__(self, key):
        """mimic dict"""
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
        return self.__data.items()

    def values(self):
        """mimic dict"""
        return self.__data.values()

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

    @property
    def data(self):
        return self.__data

    @data.setter
    def data(self, value):
        raise NotImplementedError("setting of data is not allowed, use add() or append()")
        self.__data = value

    @property
    def index_keys(self):
        return self.__index_keys

    @index_keys.setter
    def index_keys(self, value):
        raise NotImplementedError("setting of index_keys is not allowed")
        self.__index_keys = tuple(value)

#    def get_index_keys(self):
#        return tuple(self.index_keys)

    @property
    def value_keys(self):
        return self.__value_keys

    @value_keys.setter
    def value_keys(self, value):
        raise NotImplementedError("setting of value_keys is not allowed")
        self.__value_keys = list(value)

#    def get_value_keys(self):
#        return self.value_keys

    @property
    def ts_key(self):
        return self.__ts_key

    @ts_key.setter
    def ts_key(self, value):
        raise NotImplementedError("setting of ts_key is not allowed")
        self.__ts_key = value

#    def get_ts_key(self):
#        return self.ts_key

#    def get_first_ts(self):
#        first_ts = min(series[0][0] for series in self.data.values())
#        #logging.info("first ts found in data : %s", first_ts)
#        return first_ts

#    def get_last_ts(self):
#        last_ts = max(series[-1][0] for series in self.data.values())
#        #logging.info("first ts found in data : %s", last_ts)
#        return last_ts

    def add(self, data):
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
            logging.error("there are index_keys missing in this dataset, skipping")
            return
        # if this key is new, create empty Timeseries object
        if key not in self.__data:
            self.__data[key] = Timeseries(self.__value_keys)
        # add data to this timeseries object
        try:
            # timestamp and values has to be converted to float
            self.__data[key].add(float(data[self.__ts_key]), tuple((float(data[key]) for key in self.__value_keys)))
        except KeyError as exc:
            #logging.exception(exc)
            logging.error("there is some key missing in %s, should be %s and %s, skipping this dataset, skipping this dataset", data.keys(), self.__ts_key, self.__value_keys)
        except ValueError as exc:
            #logging.exception(exc)
            logging.error("some value_keys or ts_keyname are not numeric and float convertible, skipping this dataset: %s", data)

    def append(self, key, timeserie):
        """
        key <str> key to store this timeseries in dict
        timeserie <Timeseries> object that holds the data

        append whole timeserie to existing data
        data length must be the same, but start_ts and stop_ts can be slightly different for each key
        """
        #logging.debug("new start : %s, stop: %s, length %s", timeserie[0][0], timeserie[-1][0], len(timeserie))
        assert key not in self.__data
        if len(self.__data) > 0:
            #logging.debug("existing start : %s, stop: %s, length %s", self.data.values()[0][0][0], self.data.values()[0][-1][0], len(self.data.values()[0]))
            try:
                assert len(timeserie) == len(self.__data.values()[0]) # must be the same length
            except AssertionError:
                logging.error("The Timeseries Object to append, has not the same length as existing data in this array")
                logging.error("existing: %d, new %d", len(timeserie), len(self.__data.values()[0]))
        else:
            logging.debug("this is the first timeseries")
        self.__data[key] = timeserie

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
        for key in self.__data:
            subkey = key[fieldnum]
            try:
                stat_data = self.__data[key].get_stat(time_func)
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

    def add_derive_col(self, colname, newcolname):
        """
        add one to key to every Timeseries, for this specific colname
        which represents the difference between two values in time
        TODO: describe this better
        """
        assert colname in self.__value_keys
        assert newcolname not in self.__value_keys
        for key, timeseries in self.__data.items():
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
        for timeseries in self.__data.values():
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
        for timeseries in self.__data.values():
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
        for timeseries in self.__data.values():
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
        for key, timeseries in self.__data.items():
            try:
                assert colname in timeseries.headers
            except AssertionError as exc:
                logging.error("Timeseries with key %s, has no header %s, actually only %s", key, colname, timeseries.headers)
                raise exc
            timeseries.remove_col(colname)
        self.__value_keys.remove(colname)

    def slice(self, colnames):
        ret_data = TimeseriesArray(index_keys=self.__index_keys, value_keys=colnames, ts_key=self.__ts_key)
        for key, value in self.__data.items():
            ret_data.data[key] = value.slice(colnames)
        return ret_data

    def get_group_by_tsa(self, subkeys, group_func):
        """
        subkeys <tuple> tuple of index_keys, must be subset of self.headers
        group_func <func> function to use for aggragation, eg. lambda a: sum(<tuple>)

        return grouped TimeseriesArray from self

        returns <TimeseriesArray>
        """
        # TODO: make sure this object can be aggrgated
        # have alle series, the same row, the same column width and so
        # on
        new_tsa = None
        data = None
        if len(subkeys) > 0:
            for subkey in subkeys:
                try:
                    assert subkey in self.__index_keys
                except AssertionError as exc:
                    logging.error("subkey %s not in headers list of this TimeseriesArray %s", subkey, self.__index_keys)
                    raise exc
            new_tsa = TimeseriesArray(subkeys, self.__value_keys, self.__ts_key)
            data = self.__inflate_keys(subkeys)
        else:
            logging.debug("__total__ aggregation of all timeseries objects into one single Timeseries")
            new_tsa = TimeseriesArray(("__total__", ), self.__value_keys, self.__ts_key)
            data = {
                ("__total__", ) : tuple((timeseries for timeseries in self.__data.values()))
            }
        # make sure every timeserie under each subkey has the same
        # length
        for subkey in data:
            try:
                assert all((len(data[subkey][index]) == len(data[subkey][0]) for index in range(len(data[subkey]))))
            except AssertionError:
                raise StandardError("UnusableDataError Data is not groupable, datalength of timeseries differ")
        self.__aggregate_timeseries(new_tsa, data, group_func)
        return new_tsa

    def get_grouped_tsa(self, subkeys, group_func_name):
        """
        subkeys <tuple> tuple of index_keys, must be subset of self.headers
        group_func_name <str> specifies wich function should be use to aggregate data

        return grouped TimeseriesArray from self

        returns <TimeseriesArray>
        """
        assert group_func_name in self.group_funcs.keys()
        # TODO: make sure this object can be aggrgated
        # have alle series, the same row, the same column width and so
        # on
        new_tsa = None
        data = None
        if len(subkeys) > 0:
            for subkey in subkeys:
                try:
                    assert subkey in self.__index_keys
                except AssertionError as exc:
                    logging.error("subkey %s not in headers list of this TimeseriesArray %s", subkey, self.__index_keys)
                    raise exc
            new_tsa = TimeseriesArray(subkeys, self.__value_keys, self.__ts_key)
            data = self.__inflate_keys(subkeys)
        else:
            logging.debug("__total__ aggregation of all timeseries objects into one single Timeseries")
            new_tsa = TimeseriesArray(("__total__", ), self.__value_keys, self.__ts_key)
            data = {
                ("__total__", ) : tuple((timeseries for timeseries in self.__data.values()))
            }
        # make sure every timeserie under each subkey has the same
        # length
        for subkey in data:
            try:
                assert all((len(data[subkey][index]) == len(data[subkey][0]) for index in range(len(data[subkey]))))
            except AssertionError:
                raise StandardError("UnusableDataError Data is not groupable, datalength of timeseries differ")
        self.__aggregate_timeseries(new_tsa, data, self.group_funcs[group_func_name])
        return new_tsa

    def __inflate_keys(self, subkeys):
        """
        subkeys <tuple> has to be a subset of self.headers
        group_by_func <func> to aggregate a series of values
        drift_range <float> amount of time in s

        group given data by subkey, and use group_func to aggregate data

        so this is a timeseries, there could be a tiny gap of timestamp
        on grouping values, following rules apply to this behaviour

        the first timestamp counts
        timestamps in rnge of +/- drift_range count as the same timestamp
        """
        # aggregate multiple n-Timeseries for unique keys, to a subset
        # of the unique key. the list of timeseries object has to be
        # aggregated afterwards, to get a new TimeseriesArray
        # inflate keys
        ret_data = {}
        for key, timeseries in self.__data.items():
            #logging.debug("working on timeseries for key %s", key)
            index_dict = self.get_index_dict(key)
            #logging.debug("index keys of this timeseries object %s", index_dict)
            new_key_values = []
            for index_key, index_value in index_dict.items():
                if index_key in subkeys:
                    new_key_values.append(index_value)
            new_key_values = tuple(new_key_values)
            #logging.debug("created new subkey %s", new_key_values)
            # add up timeseries to list of this key
            if new_key_values in ret_data:
                ret_data[new_key_values].append(timeseries)
            else:
                ret_data[new_key_values] = [timeseries, ]
        # aggregate timeseries objects with group_by_func
        return ret_data

    @staticmethod
    def __aggregate_timeseries(tsa, data, group_func):
        """
        tsa <TimeseriesArray> usually empty
        data <dict> { tuple of timeseries }  data structure as returned by __inflate_keys
        group_func <func> function which takes tuple and returns single value

        this method aggregates data into tsa and uses group_func to aggragte timeseries objects

        modifies given tsa object
        """
        new_value_keys = list(tsa.value_keys) # TODO create new list, not reference
        new_value_keys.append("group_count")
        for subkey in data.keys():
            #logging.debug("working on subkey %s, there are %s timeseries to aggregate", subkey, len(data[subkey]))
            cols = len(data[subkey][0][0]) # as much columns
            #logging.debug("there are %d columns in every timeseries", cols)
            rows = len(data[subkey][0])
            #logging.debug("there are %d rows in every timeseries", rows)
            timeserie = Timeseries(new_value_keys)
            for rownum in range(rows):
                # define new array, beginning with ts from first timeseries,
                # first row
                row_ts = data[subkey][0][rownum][0]
                rowdata = []
                for colnum in range(1, cols):
                    try:
                        series = tuple((ts[rownum][colnum] for ts in data[subkey]))
                        rowdata.append(group_func(series))
                    except IndexError as exc:
                        logging.exception(exc)
                        logging.error("Timeseries Format Error, series must be skipped")
                if len(rowdata) > 0:
                    rowdata.append(len(data[subkey]))
                    timeserie.add(row_ts, tuple(rowdata))
            assert len(rowdata) == len(new_value_keys)
            tsa.append(subkey, timeserie)

    def sanitize(self, drift=0.05):
        """
        parameters:

        drift <float> defaults to 0.05 and means 5% drift from mean values are allowed

        checks if data is in accurate shape to be grouped
        - data has to start at nearly the same timestamp
        - data has to end at nearly the same timestamp
        - Timeseries has to be the same length
        - the time interval between two consecutive values has to be
          nearly the same

        when i speak from nearly, it means +/- about <drift>%
        """
        avg_len = numpy.median(tuple((len(timeseries) for timeseries in self.__data.values())))
        std_len = numpy.std(tuple((len(timeseries) for timeseries in self.__data.values())))
        logging.info("Average length of stored timeseries: %s", avg_len)
        logging.info("Standard deviation of length       : %s (should be near 0.0)", std_len)
        for key, timeseries in self.__data.items():
            if not len(timeseries) == int(avg_len):
                logging.info(" timeseries with key %s with len %s has not the average data length of %s", key, len(timeseries), avg_len)
                logging.error("deleting key %s: data length difference to high", key)
                del self.__data[key]
        avg_start_ts = numpy.mean(tuple((timeseries[0][0] for timeseries in self.__data.values())))
        logging.info("average start_ts of all timeseries: %s", avg_start_ts)
        for key, timeseries in self.__data.items():
            if not is_near(timeseries[0][0], avg_start_ts, drift):
                logging.info("%s start_ts of %s differs more than 5 percent", key, timeseries[0][0])
                logging.error("deleting key %s: start_ts difference to high", key)
                del self.__data[key]
        avg_stop_ts = numpy.mean(tuple((timeseries[-1][0] for timeseries in self.__data.values())))
        logging.info("average stop_ts of all timeseries: %s", avg_stop_ts)
        for key, timeseries in self.__data.items():
            if not is_near(timeseries[-1][0], avg_stop_ts, drift):
                logging.info("%s stop_ts of %s differs more than 5 percent", key, timeseries[-1][0])
                logging.error("deleting key %s: stop_ts difference to high", key)
                del self.__data[key]
        avg_interval = sum((timeseries.get_interval() for timeseries in self.__data.values())) / len(self.__data)
        std_interval = numpy.std(tuple((timeseries.get_interval() for timeseries in self.__data.values())))
        logging.info("Average interval stored timeseries : %s", avg_interval)
        logging.info("Standard deviation of interval     : %s (should be near 0.0)", std_interval)
        for key, timeseries in self.__data.items():
            if not is_near(timeseries.get_interval(), avg_interval, drift):
                logging.info(" timeseries with key %s with interval %s has not the average data interval of %s", key, timeseries.get_interval(), avg_interval)
                logging.error("deleting key %s: interval difference to high", key)
                del self.__data[key]

    def dump(self):
        """
        function to export all stored data in such a manner, that this is easily used to feed the add function of another TimeseriesArray object

        create new object tsa_new (tsa is the existing one) like

        tsa_new = TimeseriesArray(tsa.index_keys, tsa.value_keys, tsa.ts_key)

        and you can map(tsa.add(), tsa.dump())
        """
        for key, timeseries in self.__data.items():
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

    def dump_to_csv(self, filehandle):
        """
        dump all data to filehandle in csv format
        filehandle can also be gzip.open or other types

        parameters:
        filehandle <file>
        """
        outbuffer = []
        outbuffer.append(str((self.__index_keys, self.__value_keys, self.__ts_key)))
        headers = list(self.__index_keys)
        headers.append(self.__ts_key)
        headers.extend(self.__value_keys)
        outbuffer.append(";".join(headers))
        for key, timeseries in self.__data.items():
            # convert key tuple to dict
            for values in timeseries.data:
                row_values = list(key)
                row_values.extend([str(value) for value in values])
                outbuffer.append(";".join(row_values))
        filehandle.write("\n".join(outbuffer))

    def dump_split(self, outpath, overwrite=False):
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
        for key, timeseries in self.__data.items():
            ts_filename = self.get_ts_dumpfilename(key)
            # skip dump, if file exists, and overwrite=False
            if not os.path.isfile(os.path.join(outpath, ts_filename)) or overwrite:
                logging.debug("dumping key %s to filename %s", key, ts_filename)
                ts_filehandle = gzip.open(os.path.join(outpath, ts_filename), "wb")
                timeseries.dump_to_csv(ts_filehandle)
            outbuffer["ts_filenames"].append(ts_filename)
        json.dump(outbuffer, open(outfile, "wb"))

    @staticmethod
    def load_from_csv(filehandle):
        """
        load data from filehandle, dumped previously with self.dump_to_csv

        returns:
        <TimeseriesArray> object
        """
        data = filehandle.read().split("\n")
        index_keys, value_keys, ts_key = eval(data[0])
        tsa = TimeseriesArray(index_keys, value_keys, ts_key)
        headers = data[1].split(";")
        for row in data[2:]:
            row_dict = dict(zip(headers, row.split(";")))
            tsa.add(row_dict)
        return tsa

    @staticmethod
    def get_ts_dumpfilename(key):
        return "ts_%s.csv.gz" % base64.b64encode(unicode(key))

    @staticmethod
    def get_dumpfilename(index_keys):
        return "tsa_%s.json" % base64.b64encode(unicode(index_keys))

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
    def get_load_split_filenames(path, index_keys, filterkeys=None, matchtype="and"):
        """
        filterkeys could be a part of existing index_keys
        all matching keys will be used
        """
        tsa_filename = TimeseriesArray.get_dumpfilename(index_keys)
        logging.debug("tsa_filename: %s", tsa_filename)
        infile = os.path.join(path, tsa_filename)
        data = json.load(open(infile, "rb"))
        logging.debug("loaded json data")
        logging.debug("index_keys: %s", data["index_keys"])
        logging.debug("value_keys: %s", data["value_keys"])
        logging.debug("ts_key: %s", data["ts_key"])
        logging.debug("number of ts files: %s", len(data["ts_filenames"]))
        filenames = {}
        for filename in data["ts_filenames"]:
            logging.debug("reading ts from file %s", filename)
            enc_key = filename.split(".")[0][3:] # only this pattern ts_(.*).csv.gz
            key = eval(base64.b64decode(enc_key))
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
    def load_split(path, index_keys, filterkeys=None, matchtype="and"):
        """
        filterkeys could be a part of existing index_keys
        all matching keys will be used
        """
        tsa_filename = TimeseriesArray.get_dumpfilename(index_keys)
        infile = os.path.join(path, tsa_filename)
        data = json.load(open(infile, "rb"))
        tsa = TimeseriesArray(data["index_keys"], data["value_keys"], data["ts_key"])
        for key, filename in tsa.get_load_split_filenames(path, index_keys, filterkeys, matchtype).items():
            logging.debug("loading Timeseries from file %s", filename)
            tsa.data[key] = Timeseries.load_from_csv(gzip.open(filename, "rb"))
        return tsa

