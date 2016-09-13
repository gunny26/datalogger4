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
import pwd
# own modules
from datalogger.TimeseriesArrayLazy import TimeseriesArrayLazy as TimeseriesArrayLazy
from datalogger.TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger.TimeseriesStats import TimeseriesStats as TimeseriesStats
from datalogger.Quantile import QuantileArray as QuantileArray
from datalogger.CorrelationMatrix import CorrelationMatrixArray as CorrelationMatrixArray
from datalogger.CustomExceptions import DataLoggerRawFileMissing
from datalogger.CustomExceptions import DataLoggerLiveDataError
from datalogger.CustomExceptions import DataLoggerFilenameDecodeError

DEBUG = False

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
        self.__basedir = basedir
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
        try:
            self.__meta = self.__load_metainfo(metafile)
        except StandardError as exc:
            logging.exception(exc)
            logging.error("error while loading meta informations from file %s", metafile)
            raise exc
        # to local __dict__
        self.__delimiter = self.__meta["delimiter"]
        self.__ts_keyname = self.__meta["ts_keyname"]
        self.__headers = tuple(self.__meta["headers"])
        # transitional hook to implement datatypes without correcting
        # all meta files at once
        if isinstance(self.__meta["value_keynames"], dict):
            self.__value_keynames = tuple(self.__meta["value_keynames"].keys())
            self.__datatypes = self.__meta["value_keynames"]
        elif isinstance(self.__meta["value_keynames"], list):
            # if old stype, keep all datatype asis, and print warning
            self.__value_keynames = tuple(self.__meta["value_keynames"])
            self.__datatypes = dict(zip(self.__meta["value_keynames"], ("asis",) * len(self.__meta["value_keynames"])))
            logging.error("You should define value_keynames as dict with datatypes")
        self.__index_keynames = tuple(self.__meta["index_keynames"])
        self.__blacklist = tuple(self.__meta["blacklist"])
        self.__interval = self.__meta["interval"]
        # add available Statistical function names to meta structure
        self.__meta["stat_func_names"] = TimeseriesStats.stat_funcs.keys()
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
    def datatypes(self):
        """dictionary of datatypes"""
        return self.__datatypes

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

    @property
    def meta(self):
        """definition of this particular project/tablename configuration"""
        return self.__meta

    def __getitem__(self, key):
        """
        super overloaded __getitem__ function
        """
        #print key
        #print type(key)
        if isinstance(key, basestring): # only datestring given
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
            raise KeyError("key must be supplied as tuple (datestring, TimeseriesArrayLazy key, Timeseries key) or single value (datestring)")

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
        logging.debug("reading raw data from file %s", filename)
        start_ts, stop_ts = self.get_ts_for_datestring(datestring) # get first and last timestamp of this date
        logging.debug("appropriate tiemstamps for this date are between %s and %s", start_ts, stop_ts)
        #data = self.__get_file_handle(filename, "rb").read().split("\n")
        filehandle = self.__get_file_handle(filename, "rb")
        next(filehandle) # skip header line
        for lineno, row in enumerate(filehandle):
            if len(row) == 0 or row[0] == "#":
                continue
            try:
                data = self.__parse_line(unicode(row, "utf-8"), timedelta)
                if self.__ts_keyname not in data:
                    logging.info("Format Error in row: %s, got %s", row, data)
                    continue
                if not start_ts <= data[self.__ts_keyname] <= stop_ts:
                    logging.debug("Skipping line, ts %s not between %s and %s", data[self.__ts_keyname], start_ts, stop_ts)
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

    def raw_reader(self, datestring):
        """
        kind of debugging method to read from raw file, like load_tsa does,
        but report every line as is, only converted into dict
        """
        for row in self.__get_raw_data_dict(datestring, 0):
            yield row

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
        # try to set ownership of created directories
        username = self.get_user(self.__basedir)
        try:
            uid = pwd.getpwnam(username).pw_uid
            gid = pwd.getpwnam(username).pw_gid
            os.chown(os.path.join(self.__global_cachedir, datestring), uid, gid)
            os.chown(os.path.join(self.__global_cachedir, datestring, self.__project), uid, gid)
            os.chown(os.path.join(self.__global_cachedir, datestring, self.__project, self.__tablename), uid, gid)
        except KeyError as exc:
            logging.exception(exc)
            logging.error("User %s does not exist on this systemi, default permission will be applied to created directories", username)
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
        # the original raw file could be deleted, and only the
        # calculated TSA/TSASTATS and so on are available. In this case
        # define None
        try:
            caches["tsa"]["raw"] = self.__get_raw_filename(datestring) # raises exception if no file was found
        except DataLoggerRawFileMissing:
            caches["tsa"]["raw"] = None
        except StandardError as exc:
            logging.exception(exc)
            raise
        for cachetype in caches.keys():
            file_pattern = os.path.join(self.__get_cachedir(datestring), caches[cachetype]["pattern"])
            for abs_filename in glob.glob(file_pattern):
                filename = os.path.basename(abs_filename)
                key = self.__decode_filename(filename)
                caches[cachetype]["keys"][unicode(key)] = filename
        # add quantile part
        caches["quantile"] = {
            "pattern" : "quantile.json",
            "exists" : os.path.isfile(os.path.join(self.__get_cachedir(datestring), "quantile.json"))
        }
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

    def load_tsa(self, datestring, filterkeys=None, index_pattern=None, timedelta=0, cleancache=False, validate=False):
        """
        caching version to load_tsa_raw
        if never called, get ts from load_tsa_raw, and afterwards dump_tsa
        on every consecutive call read from cached version
        use cleancache to remove caches

        parameters:
        datestring <str>
        filterkeys <tuple> or None default None
        index_pattern <str> or None default None
        timedelta <int> default 0
        cleancache <bool> default False
        validate <bool> if data is read from raw, dump it after initail read,
            and reread it afterwards to make sure the stored tsa is OK
            thats an performance issue

        returns
        <TimeseriesArrayLazy> object read from cachefile or from raw data
        """
        try:
            assert not_today(datestring)
        except AssertionError:
            raise DataLoggerLiveDataError("Reading from live data is not allowed")
        cachedir = self.__get_cachedir(datestring)
        cachefilename = os.path.join(cachedir, TimeseriesArrayLazy.get_dumpfilename(self.__index_keynames))
        def fallback():
            """
            fallback method to use, if reading from cache data is not possible
            """
            tsa = self.load_tsa_raw(datestring, timedelta)
            tsa.dump_split(cachedir) # save full data
            # read the data afterwards to make sure there is no problem,
            if validate is True:
                tsa = TimeseriesArrayLazy.load_split(cachedir, self.__index_keynames, filterkeys=filterkeys, index_pattern=index_pattern, datatypes=self.__datatypes)
            # also generate TSASTATS and dump to cache directory
            tsastats = TimeseriesArrayStats(tsa) # generate full Stats
            tsastats.dump(cachedir) # save
            # and at last but not least quantile
            qantile = QuantileArray(tsa, tsastats)
            cachefilename = os.path.join(cachedir, "quantile.json")
            qantile.dump(open(cachefilename, "wb"))
            # finally return tsa
            return tsa
        if not os.path.isfile(cachefilename):
            logging.info("cachefile %s does not exist, fallback read from raw data file", cachefilename)
            return fallback()
        if (os.path.isfile(cachefilename)) and (cleancache == True):
            logging.info("deleting cachefile %s and read from raw data file", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        logging.debug("loading stored TimeseriesArrayLazy object file %s", cachefilename)
        try:
            tsa = TimeseriesArrayLazy.load_split(cachedir, self.__index_keynames, filterkeys=filterkeys, index_pattern=index_pattern, datatypes=self.__datatypes)
            return tsa
        except IOError:
            logging.error("IOError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        except EOFError:
            logging.error("EOFError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()

    def iconvert(self, tsa):
        """
        DEPRECTAED: will be done in TimeseriesArrayLazy

        convert given tsa to defined datatypes
        modifies tsa object and return the modified version

        parameters:
        tsa <TimeseriesArrayLazy>
        """
        for colname, datatype in self.__datatypes.items():
            if datatype != "asis":
                tsa.convert(colname, datatype)
        return tsa

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
        <TimeseriesArrayLazy> object read from cachefile or from raw data
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
            logging.info("cachefile %s does not exist, fallback read from tsa archive", cachefilename)
            return fallback()
        if (os.path.isfile(cachefilename)) and (cleancache == True):
            logging.info("deleting cachefile %s and read from raw", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        logging.debug("loading stored TimeseriesArrayLazy object file %s", cachefilename)
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

    def load_quantile(self, datestring):
        """
        retuns quantile for this specific tsa, either load cache version,
        or recreate from tsa

        parameters:
        datestring <str>

        returns:
        <QuantileArray>
        """
        cachedir = self.__get_cachedir(datestring)
        cachefilename = os.path.join(cachedir, "quantile.json")
        quantile_array = None
        if os.path.isfile(cachefilename):
            quantile_array = QuantileArray.load(open(cachefilename, "rb"))
        else:
            logging.info("cachefile %s does not exist, fallback read from tsa archive", cachefilename)
            tsa = self.load_tsa(datestring)
            tsa.cache = True # to enable in memory caching of timeseries
            # huge performance improvement, from 500s to 70s
            tsastats = self.load_tsastats(datestring)
            quantile_array = QuantileArray(tsa, tsastats)
            quantile_array.dump(open(cachefilename, "wb"))
        return quantile_array

    def load_correlationmatrix(self, datestring):
        """
        retuns correlattion matrix for this specific tsa, either load cache version,
        or recreate from tsa

        parameters:
        datestring <str>

        returns:
        <CorrelationMatrixArray>
        """
        cachedir = self.__get_cachedir(datestring)
        cachefilename = os.path.join(cachedir, "correlationmatrix.json")
        cma = None
        if os.path.isfile(cachefilename):
            cma = CorrelationMatrixArray.load(open(cachefilename, "rb"))
        else:
            logging.info("cachefile %s does not exist, fallback read from tsa archive", cachefilename)
            tsa = self.load_tsa(datestring)
            cma = CorrelationMatrixArray(tsa)
            cma.dump(open(cachefilename, "wb"))
        return cma

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
        try:
            parts = filename.split(".")[0].split("_")
            key_encoded = "_".join(parts[1:]) # there could be more than 2 parts
            # the first part ist something like tsa_, tsastats_, ts_,
            # tsstats_ and so on.
            #_, key_and_ending = filename.split("_")
            #key_encoded = key_and_ending.split(".")[0]
            key = None
            try:
                # TODO: there are some problems to decode b64string with
                # urlsafe_b64decode if unicode,
                # try to use b64decode instead
                try:
                    key = eval(base64.urlsafe_b64decode(str(key_encoded)))
                except TypeError as exc:
                    logging.exception(exc)
                    key = eval(base64.b64decode(key_encoded))
                assert type(key) == tuple
                return key
            except StandardError as exc:
                logging.exception(exc)
                raise DataLoggerFilenameDecodeError("filename %s could not be decoded to tuple, result: %s" % (filename, key))
        except StandardError as exc:
            logging.exception(exc)
            raise DataLoggerFilenameDecodeError("Something went wrong while decoding filensme %s" % filename)

    def load_tsa_raw(self, datestring, timedelta=0):
        """
        read data from raw input files and return TimeseriesArrayLazy object

        parameters:
        datestring <str> isodate representation of date like 2015-12-31
        timedelta <int> amount second to correct raw input timestamps

        returns:
        <TimeseriesArrayLazy> object wich holds all data of this day
        """
        tsa = TimeseriesArrayLazy(self.__index_keynames, self.__value_keynames, datatypes=self.__datatypes)
        for rowdict in self.__get_raw_data_dict(datestring, timedelta):
            try:
                tsa.add(rowdict)
            except ValueError as exc:
                logging.exception(exc)
                logging.error("ValueError by adding this data to TimeseriesArrayLazy: %s", rowdict)
                raise exc
            except AssertionError as exc:
                logging.exception(exc)
                logging.error("AssertionError by adding this data to TimeseriesArrayLazy: %s", rowdict)
                raise exc
        return tsa
    read_day = load_tsa_raw

    def group_by(self, datestring, tsa, subkeys, group_func):
        """
        TODO: make this method static
        group given tsa by subkeys, and use group_func to aggregate data
        first all Timeseries will be aligned in time, to get proper points in timeline

        parameters:
        tsa <TimeseriesArrayLazy>
        subkey <tuple> could also be empty, to aggregate everything
        group_func <func> like lambda a,b : (a+b)/2 to get averages
        slotlength <int> interval in seconds to correct every timeseries to

        returns:
        <TimeseriesArrayLazy>
        """
        # intermediated tsa
        tsa2 = TimeseriesArrayLazy(index_keys=subkeys, value_keys=tsa.value_keys, ts_key=tsa.ts_key, datatypes=self.__datatypes)
        start_ts, _ = DataLogger.get_ts_for_datestring(datestring)
        ts_keyname = tsa.ts_key
        for data in tsa.export():
            # align timestamp
            nearest_slot = round((data[ts_keyname] - start_ts) / self.__interval)
            data[ts_keyname] = int(start_ts + nearest_slot * self.__interval)
            #data[ts_keyname] = align_timestamp(data[ts_keyname])
            tsa2.group_add(data, group_func)
        return tsa2

    @staticmethod
    def tsastat_group_by(tsastat, subkey):
        """
        group given tsastat array by some subkey
        TODO: return TimeseriesArrayStats Object to be consistent

        parameters:
        tsastat <TimeseriesArrayStats>
        subkey <tuple> subkey to group by

        returns:
        <dict>
        """
        # how to aggregate statistical values
        group_funcs = {
            u'count' : lambda a, b: a + b,
            u'std' : lambda a, b: (a + b)/2,
            u'avg': lambda a, b: (a + b)/2,
            u'last' : lambda a, b: -1.0, # theres no meaning
            u'min' : min,
            u'max' : max,
            u'sum' : lambda a, b: (a + b) / 2,
            u'median' : lambda a, b: (a + b)/2,
            u'mean' : lambda a, b: (a + b)/2,
            u'diff' : lambda a, b: (a + b)/2,
            u'dec' : lambda a, b: (a + b)/2,
            u'inc' : lambda a, b: (a + b)/2,
            u'first' : lambda a, b: -1.0, # theres no meaning
        }
        #tsastat = datalogger.load_tsastats("2016-02-08")
        #print(datalogger.index_keynames)
        #print(datalogger.value_keynames)
        #groupby = ("hostname", )
        newdata = {}
        for index_key, tsstat in tsastat.items():
            #print("index_key :", index_key)
            key_dict = dict(zip(tsastat.index_keynames, index_key))
            newkey = None
            if len(subkey) == 0:
                newkey = ("__total__", )
            else:
                newkey = tuple([key_dict[key] for key in subkey])
            #print("grouped key: ", newkey)
            if newkey not in newdata:
                #print("first appearance of this index_key")
                newdata[newkey] = {}
            for value_key in tsastat.value_keynames:
                if value_key not in newdata[newkey]:
                    #print("first appearance of this value_key")
                    newdata[newkey][value_key] = dict(tsstat[value_key])
                else:
                    #print("grouping data")
                    for stat_funcname in tsstat[value_key].keys():
                        #print("statistical function: ", stat_funcname)
                        existing = float(newdata[newkey][value_key][stat_funcname])
                        #print("existing data: ", existing)
                        to_group = float(tsstat[value_key][stat_funcname])
                        #print("to add data  : ", to_group)
                        newdata[newkey][value_key][stat_funcname] = group_funcs[stat_funcname](existing, to_group)
                        #print("calculated value: ", newdata[newkey][value_key][stat_funcname])
        return newdata

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
        tsa <TimeseriesArrayLazy>
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
        return data["projects"].keys()

    @staticmethod
    def get_tablenames(basedir, project):
        """return available tablenames for projects, defined in datalogger.json"""
        data = json.load(open(os.path.join(basedir, "datalogger.json"), "rb"))
        return data["projects"][unicode(project)].keys()

    @staticmethod
    def get_user(basedir):
        """return OS user to use for file permissions, defined in datalogger.json"""
        data = json.load(open(os.path.join(basedir, "datalogger.json"), "rb"))
        return data["user"]

    @staticmethod
    def get_group(basedir):
        """return OS group to use for file permissions, defined in datalogger.json"""
        data = json.load(open(os.path.join(basedir, "datalogger.json"), "rb"))
        return data["group"]

    @staticmethod
    def get_yesterday_datestring():
        """return datestring from yesterday (24h ago)"""
        return datetime.date.fromtimestamp(time.time() - 60 * 60 * 24).isoformat()

    @staticmethod
    def get_last_business_day_datestring():
        """
        returns last businessday datestring, ignoring Feiertage
        """
        last_business_day = datetime.date.today()
        shift = datetime.timedelta(max(1, (last_business_day.weekday() + 6) % 7 - 3))
        last_business_day = last_business_day - shift
        return last_business_day.isoformat()
