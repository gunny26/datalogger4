#!/usr/bin/python
# pylint: disable=line-too-long
"""Module for class Timeseries"""
import logging
# own modules
from CustomExceptions import *
from TimeseriesStats import TimeseriesStats as TimeseriesStats


def datatype_percent(times, series):
    """
    returns series converted to datatype percent

    ever value is calculated as percentage of max value in series

    parameters:
    series <tuple> of <float>

    returns:
    <tuple> of <float> percent between 0.0 and 1.0
    """
    max_value = max(series)
    try:
        new_series = tuple((value/max_value for value in series))
        return new_series
    except ZeroDivisionError:
        return (0.0,) * len(series)

def datatype_derive(times, series):
    """
    returns series converted to datatype derive
    store only differeces between two subsequent values

    parameters:
    series <tuple> of <float>

    returns:
    <tuple> of <float>
    """
    new_series = [0.0, ]
    for index in range(1, len(series)):
        new_series.append(series[index] - series[index - 1])
    return new_series

def __datatype_counter(times, series, max_value):
    """
    generic counter datatype with parameterized max_value,
    could be either 2^32, 2^64 or something completely different
    do not use directly, use counter32, counter64 instead
    first value will always be 0.0

    valid range of values : 
        min: 0.0
        max: max_value

    parameters:
    series <tuple> of <float>

    returns:
    <tuple> of <float>
    """
    new_series = [0.0, ]
    # first value has to be in range
    if not 0.0 <= series[0] <= max_value:
        msg = "counter %f out of range at time %f, max_value: %f " % (series[0], times[0], max_value)
        logging.error(msg)
        raise AssertionError(msg)
    for index in range(1, len(series)):
        if not 0.0 <= series[index] <= max_value: # requirement for counter type
            msg = "counter %f out of range at time %f, max_value: %f " % (series[index], times[index], max_value)
            logging.error(msg)
            raise AssertionError(msg)
        duration = times[index] - times[index - 1]
        if duration > 0.0: # only if duration above zero
            derive = series[index] - series[index - 1]
            if derive < 0.0: # overflow detected
                derive = max_value - series[index - 1] + series[index]
            if derive < 0.0:
                msg = "max_value: %f, old value: %f, new value: %f, old time: %f, new time: %f" % (max_value, series[index - 1], series[index], times[index - 1], times[index])
                logging.error(msg)
                raise AssertionError(msg)
            new_series.append(derive / duration)
        else:
            new_series.append(0.0)
    return new_series

def datatype_counter32(times, series):
    """
    returns series converted to datatype counter32
    counter32 will steadily increase until overflow at 2^32 occurs
    stores only differences between to subsequent values,
    first value wil always be 0.0

    parameters:
    series <tuple> of <float>

    returns:
    <tuple> of <float>
    """
    return __datatype_counter(times, series, 2.0**32)

def datatype_counter64(times, series):
    """
    returns series converted to datatype counter64
    counter64 will steadily increase until overflow at 2^64 occurs
    stores only differences between to subsequent values

    parameters:
    series <tuple> of <float>

    returns:
    <tuple> of <float>
    """
    return __datatype_counter(times, series, 2.0**64)

def datatype_persecond(times, series):
    """
    for counter which are steadily increasing, but will reset after restart
    of some part of this system
    eg. haproxy statistics counter which increases until restart, then begin by 0
    There is naturally some data missing, from last value to next value after reset

    parameters:
    series <tuple> of <tuple>(ts:<float>, value:<float>)

    returns:
    <tuple> of <float>
    """
    new_series = [0.0, ]
    for index in range(1, len(series)):
        duration = times[index] - times[index - 1]
        if duration > 0.0: # only if duration above zero
            derive = (series[index] - series[index - 1])/duration
            new_series.append(derive)
        else: # otherwise, there could be no change in value, when no time is gone
            new_series.append(0.0)
    return new_series

def datatype_counterreset(times, series):
    """
    for counter which are steadily increasing, but will reset after restart
    of some part of this system
    eg. haproxy statistics counter which increases until restart, then begin by 0
    There is naturally some data missing, from last value to next value after reset

    in difference to counter32 or counter64 this reset can occur way below 2^32 or 2^64
    there is no upper value defined to overflow

    parameters:
    series <tuple> of <tuple>(ts:<float>, value:<float>)

    returns:
    <tuple> of <float>
    """
    new_series = [0.0, ]
    for index in range(1, len(series)):
        derive = series[index] - series[index - 1]
        if derive < 0.0:
            derive = series[index]
        new_series.append(derive)
    return new_series

def datatype_gauge32(times, series):
    """
    for counter which are steadily increasing, but will reset after restart
    of some part of this system
    eg. haproxy statistics counter which increases until restart, then begin by 0
    There is naturally some data missing, from last value to next value after reset
    this counter is not supposed to overflow, so lower next level in time,
    means there was a reset

    in difference to counterreset, gauge32 will calculate 
    difference / duration  to get some e.g. byte/s values

    parameters:
    series <tuple> of <tuple>(ts:<float>, value:<float>)

    returns:
    <tuple> of <float>
    """
    new_series = [0.0, ]
    if not series[0] >= 0.0:
        msg = "counter %f out of range at time %f" % (series[0], times[0])
        logging.error(msg)
        raise AssertionError(msg)
    for index in range(1, len(series)):
        if not series[index] >= 0.0: # requirement for counter type
            msg = "counter %f out of range at time %f" % (series[index], times[index])
            logging.error(msg)
            raise AssertionError(msg)
        duration = times[index] - times[index - 1]
        if duration > 0.0: # only if duration above zero
            derive = series[index] - series[index - 1]
            if derive < 0.0: # reset detected, only difference to counter
                derive = series[index]
            if derive < 0.0:
                msg = "old value: %f, new value: %f, old time: %f, new time: %f" % (series[index - 1], series[index], times[index - 1], times[index])
                logging.error(msg)
                raise AssertionError(msg)
            new_series.append(derive / duration)
        else:
            new_series.append(0.0)
    return new_series



class Timeseries(object):
    """
    Timeseries Object for one specific index combination

    [ ts_keyname, headers[0], headers[1], headers[2], ...]
    [ ts1, col1, col2, col3 ... ]
    [ ts2, col1, col2, col3 ....]
    ...

    all column values have to be numerical
    ts value has to be integer
    """

    datatype_mapper = {
        "derive" : datatype_derive,
        "counter32" : datatype_counter32,
        "gauge32" : datatype_gauge32,
        "counter64" : datatype_counter64,
        "counterreset" : datatype_counterreset,
        "percent" : datatype_percent,
        "persecond" : datatype_persecond,
    }

    def __init__(self, headers, ts_keyname="ts"):
        """
        headers <list> column names of values
        ts_keyname <str> name of timestamp column

        all header columns have to be strictly numeric
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.__ts_keyname = ts_keyname
        self.__headers = list([value for value in headers]) # also the number of columns
        self.__index = 0
        self.__ts_index = {}
        # define new data
        self.data = []

    @property
    def ts_keyname(self):
        """name of timestamp key"""
        return self.__ts_keyname

    @ts_keyname.setter
    def ts_keyname(self, value):
        assert value not in self.__headers
        self.__ts_keyname = value

    @property
    def headers(self):
        """return list of headers (without timestamp)"""
        return self.__headers

    @headers.setter
    def headers(self, value):
        self.__headers = list(value)

    @property
    def colnames(self):
        """return list of columns (including timestamp)"""
        colnames = [self.__ts_keyname, ] + self.__headers
        return tuple(colnames)

    @property
    def start_ts(self):
        """return first recorded timestamp"""
        return self.data[0][0]

    @property
    def stop_ts(self):
        """return last recorded timestamp"""
        return self.data[-1][0]

    @property
    def stats(self):
        """return TimeseriesStats"""
        return TimeseriesStats(self)

    @property
    def interval(self):
        """
        return median time interval between two entries
        """
        t_zero = self.data[0][0]
        return sum(((row[0] - t_zero) / (index + 1) for index, row in enumerate(self.data))) / len(self.data)

    @property
    def datatypes(self):
        return list(self.datatype_mapper.keys())

    def __eq__(self, other):
        if self.__headers != other.headers:
            raise AssertionError("headers are different")
        if self.ts_keyname != other.ts_keyname:
            raise AssertionError("ts_keyname is different")
        if len(self.data) != len(other.data):
            raise AssertionError("data length is different, self %d, other %d" % (len(self.data), len(other.data)))
        if self.data != other.data:
            raise AssertionError("data is different")
        return True

    def __len__(self):
        """
        return length of timeseries data
        """
        return len(self.data)

    def __contains__(self, value):
        """
        mimic contains behaviour
        something in self
        """
        return value in self.data

    def __iter__(self):
        """
        mimic iter behaviour
        """
        return iter(self.data)

    def __getitem__(self, key):
        """
        implement sophisticated __getitem__ function

        self[<int>row] -> returns self.data[row] -> <array>
        self[<float>timestamp] -> returns self.data[row] where self.data[*][0] == timestamp -> <array>
        self[colname] -> returns self.data[*][index of colname in self.colnames] -> <tuple>
        self[<int>row, <int>col] -> returns self.data[row][colnum] -> <float>
        self[<int>row, colname] -> returns self.data[row][index of colname in self.colnames] -> <float>
        """
        # if key is int treat it as row of data
        if isinstance(key, int):
            return self.data[key]
        # if key is double-item tuple treat it position in matrix
        elif isinstance(key, tuple):
            row, col = key
            rownum = None
            colnum = None
            # convert row to rownum, depending on type
            if type(row) == int:
                rownum = row
            elif type(row) == float:
                for index, values in enumerate(self.data):
                    if values[0] == row:
                        rownum = index
                if rownum is None:
                    raise KeyError("Timstamp %f not found in dataset" % row)
            else:
                raise KeyError("Row must be either int (index)  or float (timestamp) not %s" % type(row))
            # convert col to something useful
            if type(col) == str:
                colnum = self.colnames.index(col)
            else:
                colnum = col
            return self.data[rownum][colnum]
        # if key is float treat it as timestamp
        elif isinstance(key, float):
            try:
                return self.data[self.__ts_index[key]]
            except KeyError:
                raise KeyError("Timstamp %f not found in dataset" % key)
        # if key is text treat it as column name
        elif isinstance(key, str):
            colnum = self.colnames.index(key)
            return tuple((row[colnum] for row in self.data))
        else:
            raise KeyError("%s of type %s is no valid key" % (key, type(key)))

    def __str__headers(self, delimiter="\t"):
        """generates and returns column names string"""
        colnames = [self.__ts_keyname, ]
        colnames += self.__headers
        return delimiter.join(colnames)

    def __get_col(self, colnum):
        """
        get data series from timeseries object with column number given
        """
        for row in self.data:
            yield row[colnum]

    def __add(self, timestamp, values):
        """
        private method to add data to internal data storage
        timstamp should be float
        values should be array of float

        while adding the timstamp index will be updated
        if the timestamp is already in the internal array,
        this data will be skipped

        parameters:
        timestamp <float>
        values <iterable> of float

        raises:
        DataformatError if TypeError occurs
        """
        # finally add to datastore
        if timestamp not in self.__ts_index:
            try:
                row = [timestamp,  ] + values
                self.data.append(row)
                self.__ts_index[timestamp] = self.__index
                self.__index += 1
            except TypeError as exc:
                logging.exception(exc)
                logging.error("ts : %s, values: %s", timestamp, values)
                raise DataFormatError("TypeError: some values are not of type <float>")

    def head(self, delimiter="\t", nrows=5, headers=True):
        """return printable string for first ncols rows"""
        lbuffer = []
        if headers:
            lbuffer.append(self.__str__headers(delimiter))
        for row in self.data[:nrows]:
            lbuffer.append(delimiter.join((str(value) for value in row)))
        return "\n".join(lbuffer)

    def tail(self, delimiter="\t", nrows=5, headers=True):
        """return printable string for last ncols rows"""
        lbuffer = []
        if headers:
            lbuffer.append(self.__str__headers(delimiter))
        for row in self.data[-nrows:]:
            lbuffer.append(delimiter.join((str(value) for value in row)))
        return "\n".join(lbuffer)

    def __str__(self):
        """return printable string head(), ..., tail()"""
        lbuffer = []
        lbuffer.append(self.head())
        lbuffer.append("...")
        lbuffer.append(self.tail(headers=False))
        return "\n".join(lbuffer)

    def add(self, timestamp, values, suppress_non_steady_ts=True):
        """
        add new data to timeseries
        ts should be increasing, values have to be numeric

        this method should be used while reading from raw data, where datatypes are not for given
        for reading from trusted source as dumped CSV file you should use add_from_csv

        parameters:
        ts <float> timestamp, has to be increasing, otherwise data will be ignored
        values <tuple> of <float> the actual values for this timestamp
        suppress_non_steady_ts <bool> show messages, if timestamp is not steadily increasing, or not
        """
        try:
            assert isinstance(timestamp, float)
            assert all((isinstance(value, float) for value in values))
            assert len(values) == len(self.__headers)
        except AssertionError as exc:
            raise DataFormatError("Values %s are not the same length as format specification %s" % (values, self.__headers))
        try:
            assert self.data[-1][0] < timestamp
        except IndexError as exc:
            # if this is the first entry, there is no last_ts to compare
            pass
        except AssertionError as exc:
            if not suppress_non_steady_ts:
                logging.debug("timestamp %s is not steadily increasing, ignoring this dataset, last_ts=%s", timestamp, self.data[-1][0])
        # skip this data if timeseries already in data
        if timestamp in self.__ts_index:
            logging.debug("skipping new data, timestamp already stored")
            return
        # finally add to datastore
        self.__add(timestamp, list(values))

    def add_from_csv(self, timestamp, values):
        """
        add new data to timeseries, used to add value from trusted sources like CSV files

        parameters:
        ts <float> timestamp, has to be increasing, otherwise data will be ignored
        values <tuple> of <float> the actual values for this timestamp
        """
        self.__add(timestamp, values)

    def group_add(self, timestamp, values, group_func):
        """
        function to add new data, and if data exists, aggregate existing data with new ones
        if there is no existing data for this timestamp, simply call add()

        parameters:
        timestamp <float>
        values <tuple> of <floats>
        group_func <func> will be called with existing and new values
        suppress_non_steady_ts <bool> if non steady timestamps will be reported

        returns:
        None
        """
        assert isinstance(timestamp, float)
        assert isinstance(values, list)
        assert all((isinstance(value, float) for value in values))
        if timestamp not in self.__ts_index:
            #logging.error("first data")
            self.__add(timestamp, values)
        else:
            timestamp = self.data[self.__ts_index[timestamp]][0]
            old_data = self.data[self.__ts_index[timestamp]][1:]
            #logging.error("timestamp %s aggregating new data %s to existing %s", timestamp, values, old_data)
            new_data = tuple((group_func(old_data[index], float(values[index])) for index in range(len(values))))
            self.data[self.__ts_index[timestamp]] = (timestamp, ) + new_data

    def __get_colnum(self, colname):
        """
        return column number of given column name
        """
        return self.__headers.index(colname) + 1

    def resample(self, time_interval, func):
        """
        resample data to time interval given
        using func as aggregation function for values in between
        aggregation function is caled for every series on its own, 
        so you get a tuple with numerical values and hsa to resturn one single value

        parameters:
        time_interval <int> something above actual interval
        func - something like lambda values : sum(values)

        returns:
        <Timeseries>
        """
        ret_data = Timeseries(self.__headers) # holds return data
        first_ts = self.data[0][0]
        last_ts = None
        subsample = []
        for row in self.data:
            timestamp = row[0]
            if timestamp > (first_ts + time_interval):
                # subsample is full, aggregate
                agg_data = [0] * len(subsample[0])
                for colnum in range(len(subsample[0])):
                    agg_data[colnum] = func((row[colnum] for row in subsample))
                ret_data.add(last_ts, agg_data)
                first_ts = timestamp # new starting ts for next subsample
                subsample = []
            else:
                # build up subsample data
                subsample.append(row[1:])
            last_ts = timestamp
        return ret_data

#    def dump_list(self, value_keys=None, start_ts=None, stop_ts=None):
#        """
#        return internal Data as list of tuples
#
#        [
#            [ ts1, value1, value2, ... ],
#            [ ts2, value1, value2, ... ],
#        ]
#        """
#        if value_keys is None:
#            value_keys = self.__headers
#        try:
#            assert type(start_ts) == type(stop_ts)
#            if start_ts is not None:
#                assert type(start_ts) == int
#        except AssertionError as exc:
#            logging.exception("start_ts and stop_ts has to be the same type and int")
#            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
#            raise exc
#        colnums = tuple((self.__get_colnum(key) for key in value_keys))
#        ret_data = []
#        for row in self.data:
#            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
#                row_data = [row[0], ]
#                row_data.extend((row[index] for index in colnums))
#                ret_data.append(row_data)
#        return ret_data

#    def dump_ts_dict(self, value_keys=None, start_ts=None, stop_ts=None):
#        """
#        return iternal data as dict key = ts, with tuple values
#
#        {
#            ts1: [value1, value2, value3 ...]
#            ts2: [value1, value2, value3 ...]
#        }
#        """
#        if value_keys is None:
#            value_keys = self.__headers
#        try:
#            assert type(start_ts) == type(stop_ts)
#            if start_ts is not None:
#                assert type(start_ts) == int
#        except AssertionError as exc:
#            logging.exception("start_ts and stop_ts has to be the same type and int")
#            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
#            raise exc
#        colnums = tuple((self.__get_colnum(key) for key in value_keys))
#        ret_data = {}
#        for row in self.data:
#            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
#                ret_data[row[0]] = tuple((row[index] for index in colnums))
#        return ret_data

    def to_dict(self, value_keynames=None, start_ts=None, stop_ts=None):
        """
        return internal data as dict key = ts, of dicts value_key = value

        {
            ts1: {
                key1 : value1,
                key2 : value2,
                ...
            }
             ts2: {
                key1 : value1,
                key2 : value2,
                ...
            }
            ...
        }
        """
        if value_keynames is None:
            value_keynames = self.__headers # use all columns if None
        try:
            assert type(start_ts) == type(stop_ts)
            if start_ts is not None:
                assert type(start_ts) == int
        except AssertionError as exc:
            logging.exception("start_ts and stop_ts has to be the same type and int")
            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
            raise exc
        colnums = [self.__get_colnum(key) for key in value_keynames]
        # TODO: return sorted by timestamp as default
        for row in self.data:
            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
                # mind the -1 at value_keynames!!
                row_dict = dict(((value_keynames[index-1], row[index]) for index in colnums))
                row_dict.update({self.ts_keyname : row[0]})
                yield row_dict

    def to_csv(self, value_keynames=None, headers=True, delimiter=",", start_ts=None, stop_ts=None):
        """
        return internal data csv formatted

        value_keyname <tuple> which column names to add in data
        headers <bool>  add header row or not, default True
        delimiter <str> delimiter to use for csv, default ','
        start_ts <None> or <int> starting time to use
        stop_ts <None> or <int> stopping time to use

        start_ts and stop_ts has to be the same type
        stop_ts has to be greater than start_ts and int if not None
        """
        if value_keynames is None:
            value_keynames = self.__headers
        try:
            assert type(start_ts) == type(stop_ts)
            if start_ts is not None:
                assert type(start_ts) == int
        except AssertionError as exc:
            logging.exception("start_ts and stop_ts has to be the same type and int")
            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
            raise exc
        colnums = [self.__get_colnum(key) for key in value_keynames]
        headline = [self.ts_keyname, ]
        headline.extend(value_keynames)
        for row in self.data:
            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
                if headers is True:
                    yield "%s" % delimiter.join(headline)
                    headers = False
                yield "%s%s%s" % (row[0], delimiter, delimiter.join((str(row[index]) for index in colnums)))

    def get_serie(self, colname):
        """
        returning all values for given colname

        parameters:
        colname <str> - must be in self.colnames

        returns:
        return <tuple> of <float>
        """
        colnum = self.__get_colnum(colname)
        return tuple(self.__get_col(colnum))

    def slice(self, colnames):
        """
        return new Timeseries object with only in colnames given columns

        parameters:
        colnames <tuple>

        returns:
        <Timeseries>
        """
        assert not isinstance(colnames, str)
        ret_data = Timeseries(colnames, ts_keyname=self.ts_keyname)
        for row in self.data:
            ret_data.add(row[0], tuple((row[self.__get_colnum(colname)] for colname in colnames)))
        return ret_data

    def convert(self, colname, datatype, newcolname=None):
        """
        convert some existing columns to given datatype and add this column to Timeseries

        parameters:
        colname <str> - must be in colnames
        datatype <str> - must be in datatypes
        newcolname <str> - must not be in colnames

        returns:
        <None>
        """
        if len(self.data) == 0:
            logging.error("Empty Timeseries, nothing to convert")
            return
        colnum = self.__get_colnum(colname)
        times = [row[0] for row in self.data]
        series = [row[colnum] for row in self.data]
        newseries = self.datatype_mapper[datatype](times, series)
        if newcolname is None: # ovrewrite existing column
            self.remove_col(colname)
            self.append(colname, newseries)
        else:
            self.append(newcolname, newseries)

    def add_derive_col(self, colname, colname_d):
        self.logger.info("DEPRECATED function add_derive_col use convert(%s, 'derive', %s)", colname, colname_d)
        return self.convert(colname, "derive", colname_d)
#        """
#        add derived column to colname
#        ts colname
#        t1 value1
#        t2 value2
#        ...
#
#        add these
#
#        ts colname colname_d
#        t1 value1  0
#        t2 value2  value2-value1
#        t3 value3  value3-value2
#        """
#        colnum = self.__get_colnum(colname)
#        last_value = None
#        for row in self.data:
#            if last_value is None:
#                row.append(float(0))
#            else:
#                row.append(row[colnum] - last_value)
#            last_value = row[colnum]
#        self.__headers.append(colname_d)

    def add_per_s_col(self, colname, colname_d):
        self.logger.info("DEPRECATED function add_per_s_col use convert(%s, 'persecond', %s)", colname, colname_d)
        return self.convert(colname, "persecond", colname_d)
#        """
#        add new per second columns from existing column
#
#        ts colname colname_d
#        t1 value1  0.0 (always)
#        t2 value2  (value2-value1)/(t2-t1)
#        t3 value3  (value3-value2)/(t3-t2)
#        ...
#        """
#        colnum = self.__get_colnum(colname)
#        last_ts = None
#        last_value = None
#        for row in self.data:
#            if last_ts is None:
#                row.append(0.0)
#            else:
#                try:
#                    assert row[colnum] >= last_value
#                    assert row[0] > last_ts
#                except AssertionError:
#                    logging.error("detected overflow ts: %s to %s, value: %s to %s", last_ts, row[0], last_value, row[colnum])
#                try:
#                    row.append((row[colnum] - last_value) / (row[0] - last_ts))
#                except ZeroDivisionError:
#                    row.append(0.0)
#            last_ts = row[0]
#            last_value = row[colnum]
#        self.__headers.append(colname_d)

    def add_calc_col_single(self, colname, newcolname, func):
        """
        use func to generate colname_c from colname
        colname_c = func(colname)

        maybe useful to calculate kB/s from bytes/s values by multiplying with 1024

        parameters:
        colname <str> original existing colname
        newcolname <str> new column name added
        func <func> function which returns <float>,
            ex lambda a<float>: a<float>
        """
        assert newcolname not in self.__headers
        colnum = self.__get_colnum(colname)
        for row in self.data:
            row.append(func(row[colnum]))
        self.__headers.append(newcolname)

    def add_calc_col_full(self, newcolname, func):
        """
        use func to generate newcolname from existing data at this timestamp
        newcol = func(existing data at this timestamp)

        the parameters for func are delivered as dict

        parameters:
        newcolname <str> new column name
        func <func> function which returns <float>,
            ex lambda a<dict>: a<float>
        """
        assert newcolname not in self.__headers
        colnums = tuple((self.__get_colnum(key) for key in self.__headers))
        for row in self.data:
            row_dict = dict(((self.__headers[index], row[colnums[index]]) for index in range(len(self.__headers))))
            row.append(func(row_dict))
        self.__headers.append(newcolname)

    def remove_col(self, colname):
        """
        remove column with name colname from internal data structure

        parameters:
        colname <str> schould be in self.headers
        """
        colnum = self.__get_colnum(colname)
        for row in self.data:
            del row[colnum]
        self.__headers.remove(colname)

#    def pop(self, colnum):
#        """
#        remove colnum from data
#
#        add 1 to colnum, because the first col ts is added automatically
#        """
#        colnum += 1
#        assert colnum <= len(self.data[0])
#        for row in self.data:
#            row.pop(colnum)

    def append(self, colname, series):
        """
        append given series to internal data structure and give it the name colname

        parameters:
        colanme <str> must not be in headers
        series <tuple> of <floats> must be the same length as existing data

        returns:
        None
        """
        assert colname not in self.__headers
        if len(series) != len(self.data):
            msg = "new series of length %s, is not the same as existing datalength of %s" % (len(series), len(self.data))
            logging.error(msg)
            raise AssertionError(msg)
        for index in range(len(self.data)):
            self.data[index].append(series[index])
        self.__headers.append(colname)

    def dump(self, filehandle):
        """
        write internal data to filehandle in CSV format

        parameters:
        filename <str>
        """
        header_line = [self.__ts_keyname, ]
        header_line.extend(self.__headers)
        filehandle.write(";".join(header_line) + "\n")
        for row in self.data:
            filehandle.write(";".join((str(item) for item in row)) + "\n")

    @staticmethod
    def load(filehandle):
        """
        recreate Timeseries Object from CSV Filehandle
        """
        try:
            header = True # first line is header
            timeseries = None
            for row in filehandle:
                if header is True:
                    header_line = row.strip().split(";")
                    timeseries = Timeseries(header_line[1:], header_line[0])
                    header = False
                else:
                    values = row.strip().split(";")
                    # use faster add_from_csv to avoid time consuming
                    # type checking
                    try:
                        timeseries.add_from_csv(float(values[0]), [float(value) for value in values[1:]])
                    except ValueError as exc:
                        logging.error("Error parsing row %s", row)
                        raise exc
            return timeseries
        except IOError as exc:
            logging.exception(exc)
            logging.error("Error while reading from filehandle")
            raise exc
    load_from_csv = load
