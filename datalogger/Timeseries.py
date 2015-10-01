#!/usr/bin/python
# pylint: disable=line-too-long
"""Module for class Timeseries"""
import array
import logging

class DataFormatError(StandardError):
    """raised if format does not match"""
    pass

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
    def __init__(self, headers, ts_keyname="ts"):
        """
        headers <list> column names of values
        ts_keyname <str> name of timestamp column

        all header columns have to be strictly numeric
        """
        self.__ts_keyname = unicode(ts_keyname)
        self.__headers = list([unicode(value) for value in headers]) # also the number of columns
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
    def interval(self):
        """
        return median time interval between two entries
        """
        t_zero = self.data[0][0]
        return sum(((row[0] - t_zero) / (index + 1) for index, row in enumerate(self.data))) / len(self.data)

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
        if isinstance(key, int):
            return self.data[key]
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
        # if key is float
        elif isinstance(key, float):
            try:
                return self.data[self.__ts_index[key]]
            except KeyError:
                raise KeyError("Timstamp %f not found in dataset" % key)
        # if key is text
        elif isinstance(key, basestring):
            colnum = self.colnames.index(key)
            return tuple((row[colnum] for row in self.data))
        else:
            raise KeyError("%s of type %s is no valid key" % (key, type(key)))

    def __str__headers(self, delimiter="\t"):
        """generates and returns column names string"""
        colnames = [self.__ts_keyname, ]
        colnames += self.__headers
        return delimiter.join(colnames)

    def head(self, delimiter="\t", ncols=5, headers=True):
        """return printable string for first ncols rows"""
        lbuffer = []
        if headers:
            lbuffer.append(self.__str__headers())
        for row in self.data[:ncols]:
            lbuffer.append(delimiter.join((str(value) for value in row)))
        return "\n".join(lbuffer)

    def tail(self, delimiter="\t", ncols=5, headers=True):
        """return printable string for last ncols rows"""
        lbuffer = []
        if headers:
            lbuffer.append(self.__str__headers())
        for row in self.data[-ncols:]:
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
        self.__add(timestamp, values)

    def __add(self, timestamp, values):
        try:
            #logging.error("timstamp : %s", timestamp)
            row = [timestamp,  ] + list(values)
            #logging.error("data_list : %s", ["%s(%s)" % (item, str(type(item))) for item in row])
            #data = array.array("f")
            #data.fromlist(data_list)
            #logging.error("Data to append: %s", data)
            self.data.append(row)
            self.__ts_index[timestamp] = self.__index
            self.__index += 1
        except TypeError as exc:
            logging.exception(exc)
            logging.error("ts : %s, values: %s", timestamp, values)
            raise DataFormatError("TypeError: some values are not of type <float>")

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
        using func as aggregation function
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

    def __get_col(self, colnum):
        """
        get data series from timeseries object with column number given
        """
        for row in self.data:
            yield row[colnum]

    def dump_list(self, value_keys=None, start_ts=None, stop_ts=None):
        """
        return internal Data as list of tuples

        [
            [ ts1, value1, value2, ... ],
            [ ts2, value1, value2, ... ],
        ]
        """
        if value_keys is None:
            value_keys = self.__headers
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

    def dump_ts_dict(self, value_keys=None, start_ts=None, stop_ts=None):
        """
        return iternal data as dict key = ts, with tuple values

        {
            ts1: [value1, value2, value3 ...]
            ts2: [value1, value2, value3 ...]
        }
        """
        if value_keys is None:
            value_keys = self.__headers
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

    def dump_dict(self, value_keys=None, start_ts=None, stop_ts=None):
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
        if value_keys is None:
            value_keys = self.__headers # use all columns if None
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
        return ret_data

    def dump_csv(self, value_keynames=None, headers=True, delimiter=",", start_ts=None, stop_ts=None):
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
        return ret_data

    def get_serie(self, colname):
        """
        colname <str> the name of one value key
        return timeseries of one givel value_key
        return <tuple> of <float>
        """
        colnum = self.__get_colnum(colname)
        return tuple(self.__get_col(colnum))

    def slice(self, colnames):
        """
        return new Timeseries object with only in colnames given columns
        """
        ret_data = Timeseries(colnames, ts_keyname=self.ts_keyname)
        for row in self.data:
            ret_data.add(row[0], tuple((row[self.__get_colnum(colname)] for colname in colnames)))
        return ret_data

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
        for row in self.data:
            if last_value is None:
                row.append(float(0))
            else:
                row.append(row[colnum] - last_value)
            last_value = row[colnum]
        self.__headers.append(colname_d)

    def add_per_s_col(self, colname, colname_d):
        """
        add new per second columns from existing column

        ts colname colname_d
        t1 value1  0.0 (always)
        t2 value2  (value2-value1)/(t2-t1)
        t3 value3  (value3-value2)/(t3-t2)
        ...
        """
        colnum = self.__get_colnum(colname)
        last_ts = None
        last_value = None
        for row in self.data:
            if last_ts is None:
                row.append(0.0)
            else:
                try:
                    assert row[colnum] >= last_value
                    assert row[0] > last_ts
                except AssertionError:
                    logging.error("detected overflow ts: %s to %s, value: %s to %s", last_ts, row[0], last_value, row[colnum])
                try:
                    row.append((row[colnum] - last_value) / (row[0] - last_ts))
                except ZeroDivisionError:
                    row.append(0.0)
            last_ts = row[0]
            last_value = row[colnum]
        self.__headers.append(colname_d)

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

    def pop(self, colnum):
        """
        remove colnum from data

        add 1 to colnum, because the first col ts is added automatically
        """
        colnum += 1
        assert colnum <= len(self.data[0])
        for row in self.data:
            row.pop(colnum)

    def dump(self, filehandle):
        """
        filename <str>
        writer internal data to csv filename given
        """
        header_line = [self.__ts_keyname, ]
        header_line.extend(self.__headers)
        lbuffer = []
        lbuffer.append(";".join(header_line))
        for row in self.data:
            lbuffer.append(";".join((str(item) for item in row)))
        filehandle.write("\n".join(lbuffer))
    dump_to_csv = dump

    @staticmethod
    def load(filehandle):
        """
        recreate Timeseries Object from CSV File
        """
        data = filehandle.read().split("\n")
        header_line = data[0].split(";")
        timeseries = Timeseries(header_line[1:], header_line[0])
        for row in data[1:]:
            values = row.split(";")
            timeseries.add(float(values[0]), tuple((float(value) for value in values[1:])))
        return timeseries
    load_from_csv = load
