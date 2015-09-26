#!/usr/bin/python
import numpy
import logging
logging.basicConfig(level=logging.DEBUG)


def _ss(series):
    """Return sum of square deviations of sequence data."""
    c = float(sum(series))/len(series)
    ss = sum((x - c) ** 2 for x in series)
    return ss

def pstdev(series):
    """Calculates the population standard deviation."""
    n = len(series)
    if n < 2:
        logging.error('variance requires at least two data points')
        return(0.0)
        ValueError('variance requires at least two data points')
    ss = _ss(series)
    pvar = ss/ n # the population variance
    return pvar ** 0.5

def median(series):
    """
    return median of list of numeric values
    """
    series = sorted(series)
    if len(series) < 1:
            return None
    if len(series) % 2 == 1:
            return series[((len(series) + 1) / 2) - 1]
    else:
            return float(sum(series[(len(series) / 2) - 1:(len(series) / 2) + 1])) / 2.0

def geomean(series):
    """
    build geometrical mean
    geometric mean of [1, 2, 3, 4] is (1*2*3*4)^(1/4)
    from http://bytes.com/topic/python/answers/727876-geometrical-mean
    """
    return (reduce(lambda x, y: x * y, series)) ** (1.0 / len(series))


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
    stat_funcs = {
        "min" : lambda series: numpy.min(series),
        "max" : lambda series: numpy.max(series),
        "avg" : lambda series: numpy.average(series),
        "sum" : lambda series: numpy.sum(series),
        "std" : lambda series: numpy.std(series),
        "var" : lambda series: numpy.var(series),
        "median" : lambda series: numpy.median(series),
        "count" : lambda series: len(series),
        "first" : lambda series: series[0],
        "latest" : lambda series: series[-1],
        "mean" : lambda series: numpy.mean(series),
    }

    def __init__(self, headers, ts_keyname="ts"):
        """
        headers <list> column names of values
        ts_keyname <str> name of timestamp column

        all header columns have to be strictly numeric
        """
        assert ts_keyname not in headers
        self.ts_keyname = ts_keyname
        self.headers = headers # also the number of columns
        # define new data
        #self.data = []
        self.data = None
        self.stats = {}
        self.dirty = True # indicate that there is new data
        self.last_ts = 0

    def __len__(self):
        """
        return length of timeseries data
        """
        return(len(self.data))

    def add(self, ts, values, suppress_non_steady_ts=True):
        """
        add new data to timeseries
        ts should be increasing, values have to be numeric
        """
        if self.data is None:
            self.data = numpy.zeros((0, len(values) + 1))
        try:
            assert len(values) == len(self.headers)
        except AssertionError as exc:
            logging.error("DataFormatError with values %s", values)
            logging.exception(exc)
            raise exc
        try:
            assert self.last_ts < ts
        except AssertionError as exc:
            if suppress_non_steady_ts is not True:
                logging.debug("timestamp is not steadily increasing, ignoring this dataset")
            return()
        dataset = [ts, ]
        dataset.extend(values)
        #dataset.extend(values)
        #self.data.append(dataset)
        self.data = numpy.vstack([self.data, dataset])
        self.dirty = True
        self.last_ts = ts

    def __get_colnum(self, colname):
        """
        return column number of given column name
        """
        return(self.headers.index(colname) + 1)

    def resample(self, time_interval, func):
        """
        resample data to time interval given
        using func as aggragation function
        """
        ret_data = Timeseries(self.headers) # holds return data
        first_ts = self.data[0][0]
        last_ts = None
        subsample = []
        for row in self.data:
            ts = row[0]
            if ts > (first_ts + time_interval):
                # subsample is full, aggregate
                agg_data = [0] * len(subsample[0])
                for colnum in range(len(subsample[0])):
                    agg_data[colnum] = func((row[colnum] for row in subsample))
                ret_data.add(last_ts, agg_data)
                first_ts = ts # new starting ts for next subsample
                subsample = []
            else:
                # build up subsample data
                subsample.append(row[1:])
            last_ts = ts
        return(ret_data)

    def get_headers(self):
        """
        get column names of timeseries object
        """
        return(self.headers)

    def __get_col(self, colnum):
        """
        get data series from timeseries object with column number given
        """
        return(self.data[:,colnum])

    def dump_list(self, value_keys, start_ts=None, stop_ts=None):
        """
        return internal Data as list of tuples

        [
            [ ts1, value1, value2, ... ],
            [ ts2, value1, value2, ... ],
        ]
        """
        try:
            assert type(start_ts) == type(stop_ts)
            if start_ts is not None:
                assert type(start_ts) == int
        except AssertionError as exc:
            logging.exception("start_ts and stop_ts has to be the same type and int")
            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
            raise exc
        colnums = tuple((self.__get_colnum(key) for key in value_keys))
        ret_data = []
        for row in self.data:
            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
                row_data = [row[0], ]
                row_data.extend((row[index] for index in colnums))
                ret_data.append(row_data)
        return ret_data

    def dump_ts_dict(self, value_keys, start_ts=None, stop_ts=None):
        """
        return iternal data as dict key = ts, with tuple values

        {
            ts1: [value1, value2, value3 ...]
            ts2: [value1, value2, value3 ...]
        }
        """
        try:
            assert type(start_ts) == type(stop_ts)
            if start_ts is not None:
                assert type(start_ts) == int
        except AssertionError as exc:
            logging.exception("start_ts and stop_ts has to be the same type and int")
            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
            raise exc
        colnums = tuple((self.__get_colnum(key) for key in value_keys))
        ret_data = {}
        for row in self.data:
            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
                ret_data[row[0]] = tuple((row[index] for index in colnums))
        return ret_data

    def dump_dict(self, value_keys, start_ts=None, stop_ts=None):
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
        try:
            assert type(start_ts) == type(stop_ts)
            if start_ts is not None:
                assert type(start_ts) == int
        except AssertionError as exc:
            logging.exception("start_ts and stop_ts has to be the same type and int")
            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
            raise exc
        colnums = tuple((self.__get_colnum(key) for key in value_keys))
        ret_data = {}
        for row in self.data:
            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
                ret_data[row[0]] = dict(((value_keys[index], row[colnums[index]]) for index in range(len(value_keys))))
        return(ret_data)

    def dump_csv(self, value_keynames, headers=True, delimiter=",", start_ts=None, stop_ts=None):
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
        try:
            assert type(start_ts) == type(stop_ts)
            if start_ts is not None:
                assert type(start_ts) == int
        except AssertionError as exc:
            logging.exception("start_ts and stop_ts has to be the same type and int")
            logging.error("start_ts=%s, stop_ts=%s", start_ts, stop_ts)
            raise exc
        colnums = tuple((self.__get_colnum(key) for key in value_keynames))
        ret_data = ""
        headline = [self.ts_keyname, ]
        headline.extend(value_keynames)
        for row in self.data:
            if (start_ts is None) or (start_ts <= row[0] <= stop_ts):
                if headers is True:
                    ret_data += "%s\n" % delimiter.join(headline)
                    headers = False
                ret_data += "%s%s%s\n" % (row[0], delimiter, delimiter.join((str(row[index]) for index in colnums)))
        return(ret_data)


    def get_times(self):
        """
        get all timestamps of timeseries object, sorted
        """
        return(sorted(self.__get_col(0)))

    def get_value(self, colnum):
        """
        return tuple of given column index
        """
        return(tuple(self.__get_col(colnum)))

    def generate_stats(self):
        """
        calculate some basic statistics of timeseires object
        """
        for key in self.headers:
            colnum = self.__get_colnum(key)
            series = self.get_value(colnum)
            if len(series) > 0:
                self.stats[key] = {}
                for func_name, func in self.stat_funcs.items():
                    self.stats[key][func_name] = func(series)
            else:
                logging.error("%s %s %s", key, colnum, len(self.data))
                raise StandardError("Timeseries without data cannot have stats")
        self.dirty = False # so self.stats can be reused

    def get_stats(self):
        """
        return statistics of timeseries object, and if is_dirty is True recreate statistics
        otherwise return stored data
        """
        if self.dirty is True:
            self.generate_stats()
        return(self.stats)

    def get_stat(self, func):
        """
        func must be one of self.stat_funcs.keys()
        """
        try:
            assert func in self.stat_funcs.keys()
        except AssertionError as exc:
            logging.error("func could be one of %s" % self.stat_funcs.keys())
            raise exc
        data = {}
        if self.dirty is True:
            self.generate_stats()
        for key in self.headers:
            data[key] = self.stats[key][func]
        return(data)

    def slice(self, colnames):
        """
        return new Timeseries object with only in colnames given columns
        """
        ret_data = Timeseries(colnames, ts_keyname=self.ts_keyname)
        for row in self.data:
            ret_data.add(row[0], tuple((row[self.__get_colnum(colname)] for colname in colnames)))
        return(ret_data)

    def add_derive_col(self, colname, colname_d):
        """
        add derived column to colname
        ts colname
        t1 value1
        t2 value2
        ...

        add these

        ts colname colname_d
        t1 value1  0
        t2 value2  value2-value1
        t3 value3  value3-value2
        """
        colnum = self.__get_colnum(colname)
        last_value = None
        # add a new column to numpy array, filled with zeros
        rows, columns = self.data.shape
        self.data = numpy.c_[self.data, numpy.zeros(rows)]
        newcolnum = columns
        for row in self.data:
            if last_value is None:
                row[newcolnum] = 0.0
            else:
                row[newcolnum] = row[colnum] - last_value
            last_value = row[colnum]
        self.dirty = True

    def pop(self, colnum):
        """
        remove colnum from data

        add 1 to colnum, because the first col ts is added automatically
        """
        colnum += 1
        assert colnum <= len(self.data[0])
        self.data = numpy.delete(self.data, colnum, 1)
        self.dirty = True
