#!/usr/bin/python
# pylint: disable=line-too-long
"""
Module to handle global DataLogger things, like reading from raw data,
dumping cache files, and so on
"""
import os
import glob
import json
import logging
import datetime
import calendar
import time
import gzip
import base64
# own modules
from TimeseriesArray import TimeseriesArray as TimeseriesArray
from TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats

DEBUG = False

class DataLoggerRawFileMissing(StandardError):
    """raised, when there is no available Raw Input File"""
    pass

class DataLoggerLiveDataError(StandardError):
    """raised if there is an attempt to read from live data"""
    pass

def datestring_to_date(datestring):
    """convert datestring 2015-01-31 into date object"""
    if len(datestring) != 10:
        raise StandardError("datestring must be in form of datetime.isodate()")
    year, month, day = (int(part) for part in datestring.split("-"))
    return datetime.date(year, month, day)

def not_today(datestring):
    """return True if datestring does not match today"""
    return datetime.date.today().isoformat() != datestring

def call_logger(func):
    """Decorator to log calls to functions"""
    def inner(*args, **kwds):
        """inner function"""
        if DEBUG is True:
            logging.debug("%s(%s, %s)", func.__name__, args, kwds)
        return func(*args, **kwds)
    return inner

def deprecated(func):
    """Decorator to mark deprecated methods, log some message to .error"""
    def inner(*args, **kwds):
        """inner function"""
        logging.error("called deprecated function %s(%s, %s)", func.__name__, args, kwds)
        return func(*args, **kwds)
    return inner

def timeit(func):
    """Decorator to measure processing wall_clock time"""
    def inner(*args, **kwds):
        """inner function"""
        starttime = time.time()
        result = func(*args, **kwds)
        logging.info("call to %s done in %f seconds", func.__name__, time.time() - starttime)
        return result
    return inner


class DataLogger(object):
    """
    class to handle same work around Datalogger RAW File
    """

    def __init__(self, basedir, project, tablename):
        """
        loads some meta information of this raw data table in this project
        to get name of headers, and index columns

        prameters:
        basedir <str> path to base directory for all projects
        project <str> name of project, should also be a subdirectory in basedir
        tablename <str> specific raw data  in project,
            there should be some files in <basedir>/<project>/raw/<tablename>_<isodate>.csv
        """
        self.__project = project
        self.__tablename = tablename
        project_dir = os.path.join(basedir, project)
        # define some working directories
        self.__global_cachedir = os.path.join(basedir, "global_cache")
        self.__raw_basedir = os.path.join(project_dir, "raw")
        meta_basedir = os.path.join(project_dir, "meta")
        # try to create missing directories, beginning by most top
        for directory in (basedir, project_dir, self.__global_cachedir, self.__raw_basedir, meta_basedir):
            if not os.path.exists(directory):
                logging.info("creating directory %s", directory)
                os.mkdir(directory)
        # there are some stored information about this project tablename
        # combination
        metafile = os.path.join(meta_basedir, "%s.json" % self.__tablename)
        meta = self.__load_metainfo(metafile)
        # to local __dict__
        self.__delimiter = meta["delimiter"]
        self.__ts_keyname = meta["ts_keyname"]
        self.__headers = tuple(meta["headers"])
        self.__value_keynames = tuple(meta["value_keynames"])
        self.__index_keynames = tuple(meta["index_keynames"])
        self.__blacklist = tuple(meta["blacklist"])
        self.__interval = meta["interval"]
        # make some assertions
        # every index_keyname has to be in headers
        assert all((key in self.__headers for key in self.__index_keynames))
        # ever value_keyname has to be in headers
        assert all((key in self.__headers for key in self.__value_keynames))
        # ts_keyname has to be headers
        assert self.__ts_keyname in self.__headers

    @property
    def project(self):
        """project is the highest entity"""
        return self.__project

    @property
    def tablename(self):
        """under every project there are one or some tablenames"""
        return self.__tablename

    @property
    def delimiter(self):
        """delimiter to use to read raw input"""
        return self.__delimiter

    @property
    def ts_keyname(self):
        """keyname of timestamp"""
        return self.__ts_keyname

    @property
    def headers(self):
        """all headers, order matters"""
        return self.__headers

    @property
    def value_keynames(self):
        """keynames of value fields, have to be float convertible"""
        return self.__value_keynames

    @property
    def index_keynames(self):
        """keynames of value fields, are treated as strings"""
        return self.__index_keynames

    @property
    def blacklist(self):
        """keynames to ignore from raw input file"""
        return self.__blacklist

    @property
    def raw_basedir(self):
        """subdirectory under wich to find raw inout files"""
        return self.__raw_basedir

    @property
    def global_cachedir(self):
        """subdirectory where to put caches"""
        return self.__global_cachedir

    def __getitem__(self, key):
        """
        super overloaded __getitem__ function
        """
        #print key
        #print type(key)
        if type(key) == str: # only datestring given
            return self.load_tsa(key)
        if type(key) == tuple: # tsa key selection given
            if len(key) == 2:
                tsa_key = key[1]
                tsa_key_dict = dict(zip(self.__index_keynames, tsa_key))
                if None in tsa_key: # return multiple ts in tsa
                    return self.load_tsa(key[0], filterkeys=tsa_key_dict)
                else: # returns one single ts in tsa
                    return self.load_tsa(key[0], filterkeys=tsa_key_dict)[tsa_key]
            if len(key) > 2: # tsa and ts selection given
                datestring = key[0]
                tsa_key = key[1]
                tsa_key_dict = dict(zip(self.__index_keynames, tsa_key))
                ts_key = key[2:]
                # convert into single value if only one exists
                if len(ts_key) == 1:
                    ts_key = ts_key[0]
                tsa = self.load_tsa(datestring, filterkeys=tsa_key_dict)
                return tsa[tsa_key].__getitem__(ts_key)
        else:
            raise KeyError("key must be supplied as tuple (datestring, TimeseriesArray key, Timeseries key) or single value (datestring)")

    def __load_metainfo(self, metafile):
        """
        load some meta informations for this project/tablename combination from stored json file
        this file has to exist

        parameters:
        filename <str> filename of json information

        returns:
        <dict> of stored json data
        """
        if os.path.isfile(metafile):
            logging.debug("loading meta information from %s", metafile)
            meta = json.load(self.__get_file_handle(metafile, "rb"))
            logging.debug("loaded %s", meta)
        else:
            logging.error("You have to define meta data for this tablename in %s", metafile)
            raise StandardError("You have to define meta data for this tablename in %s" % metafile)
        return meta

    def __parse_line(self, row, timedelta=0.0):
        """
        specialized method to parse a single line read from raw CSV

        parameters:
        row <str> result from readline()
        timedelta <int> amount second to correct every read timestamp

        returns:
        <dict> keys from headers, all values are converted to float
        """
        data = dict(zip(self.__headers, row.split(self.__delimiter)))
        try:
            data[self.__ts_keyname] = int(float(data[self.__ts_keyname]) + timedelta)
        except ValueError as exc:
            logging.exception(exc)
            logging.error("ValueError on row skipping this data: %s", str(data))
        except KeyError as exc:
            logging.exception(exc)
            logging.error("KeyError on row, skipping this data: %s", str(data))
        return data

    @staticmethod
    def __get_file_handle(filename, mode):
        """
        return filehandle either for gzip or normal uncompressed file

        parameters:
        filename <str> fileanme
        mode <str> as used in open(<filename>, <mode>)

        returns:
        <file> handle to opened file, either gzip.open or normal open
        """
        if filename.endswith(".gz"):
            return gzip.open(filename, mode)
        else:
            return open(filename, mode)

    def __get_raw_filename(self, datestring):
        """
        return filename of raw input file, if one is available
        otherwise raise Exception

        parameters:
        datestring <str>
        """
        filename = os.path.join(self.__raw_basedir, "%s_%s.csv" % (self.__tablename, datestring))
        if not os.path.isfile(filename):
            filename += ".gz" # try gz version
            if not os.path.isfile(filename):
                raise DataLoggerRawFileMissing("No Raw Input File named %s (or .gz) found", filename)
        return filename

    def __get_raw_data_dict(self, datestring, timedelta):
        """
        generator to return parsed lines from raw file of one specific datestring

        parameters:
        datestring <str> isodate string like 2014-12-31
        timedelta <int> amount of seconds to correct every timestamp in raw data

        yields:
        <dict> of every row
        """
        filename = self.__get_raw_filename(datestring)
        start_ts, stop_ts = self.get_ts_for_datestring(datestring) # get first and last timestamp of this date
        data = self.__get_file_handle(filename, "rb").read().split("\n")
        for lineno, row in enumerate(data[1:]):
            if len(row) == 0 or row[0] == "#":
                continue
            try:
                data = self.__parse_line(row, timedelta)
                if self.__ts_keyname not in data:
                    logging.info("Format Error in row: %s, got %s", row, data)
                    continue
                if not start_ts <= data[self.__ts_keyname] <= stop_ts:
                    continue
                yield data
            except KeyError as exc:
                logging.exception(exc)
                logging.error("KeyError in File %s, line %s, on row: %s, skipping", filename, lineno, row)
            except IndexError as exc:
                logging.exception(exc)
                logging.error("IndexError in File %s, line %s, on row: %s, skipping", filename, lineno, row)
            except UnicodeDecodeError as exc:
                logging.exception(exc)
                logging.error("UnicodeDecodeError in File %s, line %s, on row: %s, skipping", filename, lineno, row)

    def __get_cachedir(self, datestring):
        """
        return specific subdirectory to store cache files
        if this directory does not exist, ist will be created

        parameters:
        datestring <str> actual datestring

        returns:
        <str> directory path
        """
        subdir = os.path.join(self.__global_cachedir, datestring, self.__project, self.__tablename)
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        return subdir

    def get_caches(self, datestring):
        """
        search for available Timeseries cachefiles (grouped and ungrouped) and
        return references to call reading method

        parameters:
        datestring <str>

        returns:
        <func> <tuple>

        you can use the return values to simple call reading function as <func>(*<tuple>)
        """
        caches = {
            "tsa" : {
                "pattern" : "tsa_*.json",
                "keys" : {},
                "raw" : None,
            },
            "ts" : {
                "pattern" : "ts_*.csv.gz",
                "keys" : {},
            },
            "tsastat" : {
                "pattern" : "tsastat_*.json",
                "keys" : {},
            },
            "tsstat" : {
                "pattern" : "tsstat_*.json",
                "keys" : {},
            }
        }
        try:
            caches["tsa"]["raw"] = self.__get_raw_filename(datestring) # raises exception if no file was found
        except DataLoggerRawFileMissing:
            return caches
        except StandardError as exc:
            logging.exception(exc)
            raise
        for cachetype in caches.keys():
            file_pattern = os.path.join(self.__get_cachedir(datestring), caches[cachetype]["pattern"])
            for abs_filename in glob.glob(file_pattern):
                filename = os.path.basename(abs_filename)
                key = self.__decode_filename(filename)
                caches[cachetype]["keys"][key] = filename
        return caches

    def list_ts_caches(self, datestring):
        """
        search for available Timeseries cachefiles (grouped and ungrouped) and
        return references to call reading method

        parameters:
        datestring <str>

        returns:
        <func> <tuple>

        you can use the return values to simple call reading function as <func>(*<tuple>)
        """
        file_pattern = os.path.join(self.__get_cachedir(datestring), "ts_*.csv.gz")
        files = glob.glob(file_pattern)
        calls = []
        for abs_filename in files:
            filename = os.path.basename(abs_filename)
            logging.debug("found file %s", filename)
            key = self.__decode_filename(filename)
            logging.debug("found ts for key %s", key)
            calls.append((self.load_tsa, (datestring, key)))
        return calls

    def list_tsstat_caches(self, datestring):
        """
        search for available TimeseriesStats cachefiles (grouped and ungrouped) and
        return references to call reading method

        parameters:
        datestring <str>

        returns:
        <func> <tuple>

        you can use the return values to simple call reading function as <func>(*<tuple>)
        """
        file_pattern = os.path.join(self.__get_cachedir(datestring), "tsstat_*.json")
        files = glob.glob(file_pattern)
        calls = []
        for abs_filename in files:
            filename = os.path.basename(abs_filename)
            logging.debug("found file %s", filename)
            key = self.__decode_filename(filename)
            logging.debug("found tsstats for key %s", key)
            calls.append((self.load_tsastats, (datestring, key)))
        return calls

    def load_tsa(self, datestring, filterkeys=None, timedelta=0, cleancache=False):
        """
        caching version to load_tsa_raw
        if never called, get ts from load_tsa_raw, and afterwards dump_tsa
        on every consecutive call read from cached version
        use cleancache to remove caches

        parameters:
        datestring <str>
        timedelta <int>
        cleancache <bool>

        returns
        <TimeseriesArray> object read from cachefile or from raw data
        """
        try:
            assert not_today(datestring)
        except AssertionError:
            raise DataLoggerLiveDataError("Reading from live data is not allowed")
        cachedir = self.__get_cachedir(datestring)
        cachefilename = os.path.join(cachedir, TimeseriesArray.get_dumpfilename(self.__index_keynames))
        def fallback():
            """
            fallback method to use, if reading from cache data is not possible
            """
            tsa = self.load_tsa_raw(datestring, timedelta)
            tsa.dump_split(cachedir) # save full data
            tsa = TimeseriesArray.load_split(cachedir, self.__index_keynames, filterkeys)
            # also generate statistics, so its done
            tsastats = TimeseriesArrayStats(tsa) # generate full Stats
            tsastats.dump(cachedir) # save
            return tsa
        if not os.path.isfile(cachefilename):
            logging.info("cachefile %s does not exist, fallback read from raw", cachefilename)
            return fallback()
        if (os.path.isfile(cachefilename)) and (cleancache == True):
            logging.info("deleting cachefile %s and read from raw", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        logging.debug("loading stored TimeseriesArray object file %s", cachefilename)
        try:
            tsa = TimeseriesArray.load_split(cachedir, self.__index_keynames, filterkeys)
            return tsa
        except IOError:
            logging.error("IOError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        except EOFError:
            logging.error("EOFError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()

    def load_tsastats(self, datestring, filterkeys=None, timedelta=0, cleancache=False):
        """
        caching version to load_tsa_raw
        if never called, get ts from load_tsa_raw, and afterwards dump_tsa
        on every consecutive call read from cached version
        use cleancache to remove caches

        parameters:
        datestring <str>
        timedelta <int>
        cleancache <bool>

        returns
        <TimeseriesArray> object read from cachefile or from raw data
        """
        try:
            assert not_today(datestring)
        except AssertionError:
            raise DataLoggerLiveDataError("Reading from live data is not allowed")
        cachedir = self.__get_cachedir(datestring)
        cachefilename = os.path.join(cachedir, TimeseriesArrayStats.get_dumpfilename(self.__index_keynames))
        def fallback():
            """
            fallback method to use, if reading from cache data is not possible
            """
            tsa = self.load_tsa(datestring=datestring, filterkeys=None, timedelta=timedelta) # load full tsa, and generate statistics
            tsastats = TimeseriesArrayStats(tsa) # generate full Stats
            tsastats.dump(cachedir) # save
            tsastats = TimeseriesArrayStats.load(cachedir, self.__index_keynames, filterkeys=filterkeys) # read specific
            return tsastats
        if not os.path.isfile(cachefilename):
            logging.info("cachefile %s does not exist, fallback read from raw", cachefilename)
            return fallback()
        if (os.path.isfile(cachefilename)) and (cleancache == True):
            logging.info("deleting cachefile %s and read from raw", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        logging.debug("loading stored TimeseriesArray object file %s", cachefilename)
        try:
            tsastats = TimeseriesArrayStats.load(cachedir, self.__index_keynames, filterkeys=filterkeys)
            return tsastats
        except IOError:
            logging.error("IOError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        except EOFError:
            logging.error("EOFError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()

    @staticmethod
    def __decode_filename(filename):
        """
        return parameters from encoded filename (basename) in form of
        <prefix identifier>_<base64 encoded key>.<endings>

        parameters:
        filename <str> basename of file, without path

        returns:
        <tuple> decoded key (eval(base64.b64decode(key)))
        """
        _, key_and_ending = filename.split("_")
        key_encoded = key_and_ending.split(".")[0]
        key = eval(base64.b64decode(key_encoded))
        assert type(key) == tuple
        return key

    def load_tsa_raw(self, datestring, timedelta=0):
        """
        read data from raw input files and return TimeseriesArray object

        parameters:
        datestring <str> isodate representation of date like 2015-12-31
        timedelta <int> amount second to correct raw input timestamps

        returns:
        <TimeseriesArray> object wich holds all data of this day
        """
        tsa = TimeseriesArray(self.__index_keynames, self.__value_keynames)
        for rowdict in self.__get_raw_data_dict(datestring, timedelta):
            try:
                tsa.add(rowdict)
            except ValueError as exc:
                logging.exception(exc)
                logging.error("ValueError by adding this data to TimeseriesArray: %s", rowdict)
                raise exc
            except AssertionError as exc:
                logging.exception(exc)
                logging.error("AssertionError by adding this data to TimeseriesArray: %s", rowdict)
                raise exc
        return tsa
    read_day = load_tsa_raw

    def group_by(self, datestring, tsa, subkeys, group_func):
        """
        group given tsa by subkeys, and use group_func to aggregate data
        first all Timeseries will be aligned in time, to get proper points in timeline

        parameters:
        tsa <TimeseriesArray>
        subkey <tuple> could also be empty, to aggregate everything
        group_func <func> like lambda a,b : (a+b)/2 to get averages
        slotlength <int> interval in seconds to correct every timeseries to

        returns:
        <TimeseriesArray>
        """
        # intermediated tsa
        tsa2 = TimeseriesArray(index_keys=subkeys, value_keys=tsa.value_keys, ts_key=tsa.ts_key)
        start_ts, _ = self.get_ts_for_datestring(datestring)
        ts_keyname = tsa.ts_key
        for data in tsa.export():
            # align timestamp
            nearest_slot = round((data[ts_keyname] - start_ts) / self.__interval)
            data[ts_keyname] = int(start_ts + nearest_slot * self.__interval)
            #data[ts_keyname] = align_timestamp(data[ts_keyname])
            tsa2.group_add(data, group_func)
        return tsa2

    def get_wikiname(self):
        """
        returns:
        <str> it-wiki wiki name of this DataLogger

        DataLoggerReport<project><tablename>
        """
        return "DataLoggerReport%s%s" % (self.__project.capitalize(), self.__tablename.capitalize())

    @staticmethod
    def get_scatter_data(tsa, value_keys, stat_func):
        """
        get data structure to use for highgraph scatter plots,
        [
            {
                name : str(<key>),
                data : [stat_func(tsa[key][value_keys[0]]), stat_func(tsa[key][value_keys[1]], ]
            },
            ...
        ]

        parameters:
        tsa <TimeseriesArray>
        value_keys <tuple> with len 2, represents x- and y-axis
        stat_fuc <str> statistical function to use to aggregate xcolumn and ycolumns
            must exist in Timeseries object

        returns:
        <list> of <dict> data structure to use directly in highgraph scatter plots, when json encoded
        """
        assert len(value_keys) == 2
        highchart_data = []
        for key in tsa.keys():
            stats = tsa[key].get_stat(stat_func)
            highchart_data.append({
                "name" : key[0],
                "data" : [[stats[value_keys[0]], stats[value_keys[1]]],]
            })
        return highchart_data

    @staticmethod
    def datestring_to_date(datestring):
        """function to convert datestring to datetime object"""
        year, month, day = datestring.split("-")
        return datetime.date(int(year), int(month), int(day))

    @staticmethod
    def datewalker(datestring_start, datestring_stop):
        """
        function to walk from beginning datestring to end datestring,
        in steps of one day
        """
        start_date = DataLogger.datestring_to_date(datestring_start)
        stop_date = DataLogger.datestring_to_date(datestring_stop)
        while start_date <= stop_date:
            yield start_date.isoformat()
            start_date = start_date + datetime.timedelta(days=1)

    @staticmethod
    def monthwalker(monthdatestring):
        """
        funtion to walkf from first day to last day in given month
        """
        year, month = monthdatestring.split("-")
        lastday = calendar.monthrange(int(year), int(month))[1]
        start = "%04d-%02d-01" % (int(year), int(month))
        stop = "%04d-%02d-%02d" % (int(year), int(month), lastday)
        return DataLogger.datewalker(start, stop)

    def get_tsastats_longtime_hc(self, monthstring, key, value_key):
        """
        method to get longtime data from stored TimeseriesArrayStats objects
        and return data usable as higcharts input
        """
        # datalogger = DataLogger(BASEDIR, project, tablename)
        filterkeys = dict(zip(self.__index_keynames, key))
        logging.debug("build filterkeys %s", filterkeys)
        ret_data = {}
        for datestring in self.monthwalker(monthstring):
            logging.debug("getting tsatstats for %s", monthstring)
            try:
                tsastats = self.load_tsastats(datestring, filterkeys=filterkeys)
                for funcname in tsastats[key][value_key].keys():
                    if funcname in ret_data:
                        ret_data[funcname].append((datestring, tsastats[key][value_key][funcname]))
                    else:
                        ret_data[funcname] = [(datestring, tsastats[key][value_key][funcname]), ]
            except DataLoggerRawFileMissing as exc:
                logging.exception(exc)
                logging.error("No Input File for datestring %s found, skipping this date", datestring)
            except DataLoggerLiveDataError as exc:
                logging.exception(exc)
                logging.error("Reading from live data is not allowed, skipping this data, and ending loop")
                break
        return ret_data

    @staticmethod
    def get_ts_for_datestring(datestring):
        """
        returns first and last available timestamp of this date

        parameters:
        datestring <str> in isodate format like 2015-12-31

        returns:
        <int> first -> 2015-12-31 00:00:00.0
        <int> last -> 2015-12-31 23:59:59.999
        """
        def datetime_to_ts(datetime_object):
            """
            return unix timestamp from given datetime object

            parameters:
            datetime_object <datetime>

            returns:
            <int> timestamp of this datetime
            """
            return int((datetime_object - datetime.datetime.fromtimestamp(0)).total_seconds())
        year, month, day = (int(part) for part in datestring.split("-"))
        start = datetime.datetime(year, month, day, 0, 0, 0)
        start_ts = datetime_to_ts(start)
        stop = datetime.datetime(year, month, day, 23, 59, 59)
        stop_ts = datetime_to_ts(stop)
        # time.time() differs from datetime.datetime.now()
        time_to_datetime_delta = time.time() - (datetime.datetime.now() - datetime.datetime.fromtimestamp(0)).total_seconds()
        return (start_ts + time_to_datetime_delta, stop_ts + time_to_datetime_delta)

    @staticmethod
    def get_projects(basedir):
        """return available project, defined in datalogger.json"""
        data = json.load(open(os.path.join(basedir, "datalogger.json"), "rb"))
        return data.keys()

    @staticmethod
    def get_tablenames(basedir, project):
        """return available tablenames for projects, defined in datalogger.json"""
        data = json.load(open(os.path.join(basedir, "datalogger.json"), "rb"))
        return data[unicode(project)].keys()
