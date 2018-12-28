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
import pwd
import shutil
# non std
import yaml
# own modules
from datalogger3.TimeseriesArray import TimeseriesArray as TimeseriesArray
from datalogger3.TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger3.TimeseriesStats import TimeseriesStats as TimeseriesStats
from datalogger3.Quantile import QuantileArray as QuantileArray
from datalogger3.CustomExceptions import *
from datalogger3.b64 import b64encode, b64decode, b64eval
# from datalogger3.FastTsa import fast_tsa

class DataLogger(object):
    """
    class to handle same work around Datalogger Files
    either precalculate Timeseries, TimeseriesArray, TimeseriesStats and TimeseriesArrayStats
    or simple return this precalculated data

    most of the time the pre-calculation will be done with the first call for this kind of data
    """

    def __init__(self, basedir, configfilename="datalogger.json"):
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
        # checking and loading config
        if not os.path.isdir(self.__basedir):
            raise AttributeError("global Base Directory %s does not exist" % self.__basedir)
        self.__config_filename = os.path.join(basedir, configfilename)
        # loading global configuration
        with open(self.__config_filename, "rt") as infile:
            self.__config = json.load(infile)
        if "cachedir" not in self.__config:
            logging.info("you should define 'cachedir' in main configuration file, fallback to global_cache subdir under basedir")
            self.__config["cachedir"] = os.path.join(basedir, "global_cache") # TODO: remove fallback in future
        else:
            self.__config["cachedir"] = os.path.join(basedir, self.__config["cachedir"])
        if not os.path.isdir(self.__config["cachedir"]):
            raise AttributeError("global Cache Directory %s does not exist" % self.__config["cachedir"])
        self.__datestring = None
        self.__project = None
        self.__tablename = None
        self.__timedelta = None
        self.__meta = None
        self.__memcache = None 

    def setup(self, project, tablename, datestring, timedelta=0.0):
        """
        set datalogger to some specific project/tablename/datestring combination

        project <str> has to be in defined projects
        tablename <str> has to be in defined tablenames of project
        datestring <str> some datestring like 2018-12-31 in the past
        timedelta <int> defaults to 0
        """
        try:
            assert datetime.date.today().isoformat() != datestring
        except AssertionError:
            raise DataLoggerLiveDataError("Reading from live data is not allowed")
        if project == self.__project and tablename == self.__tablename and datestring == self.__datestring and self.__timedelta == timedelta:
            logging.info("DataLogger already setup for this configuration")
            return
        # cleanup memcache
        self.__memcache_init()
        self.__datestring = datestring
        self.__project = project
        self.__tablename = tablename
        self.__timedelta = timedelta
        if project not in self.__config["projects"]:
            raise AttributeError("called project %s is not defined in main configuration file" % project)
        if tablename not in self.__config["projects"][project]:
            raise AttributeError("called tablename %s is not defined in project" % tablename)
        # loading specific configuration, first look for project directory
        projectdir = os.path.join(self.__basedir, project)
        if not os.path.isdir(projectdir):
            raise AttributeError("project Directory %s does not exist" % projectdir)
        # load table definition from meta subdir, dir and subdir must exist
        # define some working directories
        metadir = os.path.join(projectdir, "meta")
        if not os.path.isdir(metadir):
            raise AttributeError("project meta directory %s does not exist" % metadir)
        metafile = os.path.join(metadir, "%s.json" % self.__tablename)
        if not os.path.isfile(metafile):
            raise AttributeError("table definition file %s does not exist" % metafile)
        with open(metafile, "rt") as infile:
            self.__meta = json.load(infile)
        # where to find raw input files
        self.__meta["raw_basedir"] = os.path.join(projectdir, "raw")
        if not os.path.isdir(self.__meta["raw_basedir"]):
            raise AttributeError("project raw input directory %s does not exist" % self.__meta["raw_basedir"])
        # if headers is dictionary, this should be treated ad description
        # TODO: this should be default in near future
        if isinstance(self.__meta["headers"], dict):
            self.__meta["decsription"] = self.__meta["headers"]
        # convert to tuple
        self.__meta["headers"] = tuple(self.__meta["headers"])
        # transitional hook to implement datatypes without correcting
        # all meta files at once
        if isinstance(self.__meta["value_keynames"], dict):
            self.__meta["datatypes"] = self.__meta["value_keynames"]
            self.__meta["value_keynames"] = tuple(self.__meta["value_keynames"].keys())
        elif isinstance(self.__meta["value_keynames"], list):
            # if old stype, keep all datatype asis, and print warning
            logging.error("You should define value_keynames as dict with datatypes")
            self.__meta["datatypes"] = dict(zip(self.__meta["value_keynames"], ("asis",) * len(self.__meta["value_keynames"])))
            self.__meta["value_keynames"] = tuple(self.__meta["value_keynames"])
        self.__meta["index_keynames"] = tuple(self.__meta["index_keynames"])
        self.__meta["blacklist"] = tuple(self.__meta["blacklist"])
        # add available Statistical function names to meta structure
        self.__meta["stat_func_names"] = list(TimeseriesStats.stat_funcs.keys())
        # make some assertions
        # every index_keyname has to be in headers
        assert all((key in self.__meta["headers"] for key in self.__meta["index_keynames"]))
        # ever value_keyname has to be in headers
        assert all((key in self.__meta["headers"] for key in self.__meta["value_keynames"]))
        # ts_keyname has to be in headers
        assert self.__meta["ts_keyname"] in self.__meta["headers"]
        # dump new style yaml file
        self._convert_to_yaml()
        # verify it
        self._verify_yaml_config()

    def __memcache_set(self, key, value):
        """ raises AtributeError if value is None """
        if value is None:
            raise AttributeError("None value is not permitted")
        self.__memcache[key] = value

    def __memcache_get(self, key):
        """ raises KeyError if key does not exist """
        return self.__memcache[key]

    def __memcache_init(self):
        self.__memcache = {}

    def __str__(self):
        ret = {
            "basedir" : self.__basedir,
            "project" : self.__project,
            "tablename" : self.__tablename,
            "config" : self.__config,
            "meta" : self.__meta,
        }
        return json.dumps(ret, indent=4)

    def _convert_to_yaml(self):
        """
        dump actual config to new style yaml file,
        only if this yaml file does not exist yet

        it is safe to call this function also if yaml file exists

        TODO: remove this method when every config file is converted
        """
        metadir = os.path.join(self.basedir, self.project, "meta")
        metafile = os.path.join(metadir, "%s.yaml" % self.tablename)
        meta = {
            "interval" : self.interval,
            "description": {},
            "delimiter" : self.delimiter,
        }
        description = meta["description"]
        for colpos, header in enumerate(self.headers):
            if header in self.value_keynames:
                coltype = "value"
            elif header in self.index_keynames:
                coltype = "index"
            elif header == self.ts_keyname:
                coltype = "ts"
            elif header in self.blacklist:
                coltype = "blacklist"
            else:
                coltype = "unknown"
            if header in self.datatypes:
                datatype = self.datatypes[header]
            else:
                datatype = None
            description[header] = {
                "colpos": colpos,
                "coltype": coltype,
                "datatype": datatype,
                "label_text": "some text to show as label text",
                "label_unit": "something/s"
            }
        if not os.path.isfile(metafile):
            logging.info("writing %s", metafile)
            with open(metafile, "wt") as outfile:
                outfile.write(yaml.dump(meta))

    def _verify_yaml_config(self):
        """
        method to verify new style yaml file against old style config
        TODO: rmeove this in future, if every config is yaml style
        """
        metadir = os.path.join(self.basedir, self.project, "meta")
        metafile = os.path.join(metadir, "%s.yaml" % self.tablename)
        if os.path.isfile(metafile):
            logging.info("loading yaml style file %s", metafile)
            with open(metafile, "rt") as infile:
                meta = yaml.load(infile)
            try: 
                assert meta["interval"] == self.interval
                assert meta["delimiter"] == self.delimiter
                description = meta["description"]
                index_keynames = tuple([key for key in description if description[key]["coltype"] == "index"])
                #print("index_keynames:", index_keynames)
                assert index_keynames == self.index_keynames
                value_keynames = tuple([key for key in description if description[key]["coltype"] == "value"])
                #print("value_kenames:", value_keynames)
                assert sorted(value_keynames) == sorted(self.value_keynames)
                ts_keyname = [key for key in description if description[key]["coltype"] == "ts"][0]
                #print("ts_keyname:", ts_keyname)
                assert ts_keyname == self.ts_keyname
                datatypes = dict([(key, description[key]["datatype"]) for key in description if description[key]["coltype"] == "value"])
                #print("datatypes:", datatypes)
                assert datatypes == self.datatypes
                blacklist = tuple([key for key in description if description[key]["coltype"] == "blacklist"])
                #print("blacklist:", blacklist)
                assert datatypes == self.datatypes
                headers_unsorted = [(key, description[key]["colpos"]) for key in description if description[key]["colpos"] is not None]
                headers = tuple([item[0] for item in sorted(headers_unsorted, key=lambda item: item[1])])
                assert headers == self.headers
                #print("headers:", headers)
                label_texts = dict([(key, description[key]["label_text"]) for key in description])
                #print("label:", label_texts)
                label_units = dict([(key, description[key]["label_unit"]) for key in description])
                #print("label units:", label_units)
                logging.info("new style yaml config %s is verified", metafile)
            except AssertionError as exc:
                logging.exception(exc)
                logging.error("new style config in %s is not the same as old style one", metafile)
                logging.error(json.dumps(meta, indent=4))
        else:
            print("new yaml config file %s not found" % metafile)

    @property
    def basedir(self):
        """basedir on underlying OS"""
        return self.__basedir

    @property
    def project(self):
        """project is the highest entity"""
        return self.__project

    @property
    def tablename(self):
        """under every project there are one or some tablenames"""
        return self.__tablename

    @property
    def datestring(self):
        return self.__datestring

    @property
    def timedelta(self):
        return self.__timedelta

    @property
    def delimiter(self):
        """delimiter to use to read raw input"""
        return self.__meta["delimiter"]

    @property
    def ts_keyname(self):
        """keyname of timestamp"""
        return self.__meta["ts_keyname"]

    @property
    def headers(self):
        """all headers, order matters"""
        return self.__meta["headers"]

    @property
    def value_keynames(self):
        """keynames of value fields, have to be float convertible"""
        return self.__meta["value_keynames"]

    @property
    def datatypes(self):
        """dictionary of datatypes"""
        return self.__meta["datatypes"]

    @property
    def index_keynames(self):
        """keynames of value fields, are treated as strings"""
        return self.__meta["index_keynames"]

    @property
    def blacklist(self):
        """keynames to ignore from raw input file"""
        return self.__meta["blacklist"]

    @property
    def raw_basedir(self):
        """subdirectory under wich to find raw inout files"""
        return self.__meta["raw_basedir"]

    @property
    def raw_filename(self):
        """raw filename or Exception"""
        return self.__get_raw_filename()

    @property
    def global_cachedir(self):
        """subdirectory where to put caches"""
        return self.__config["cachedir"]

    @property
    def meta(self):
        """definition of this particular project/tablename configuration"""
        return self.__meta

    @property
    def os_user(self):
        """return OS user to use for file permissions, defined in datalogger.json"""
        return self.__config["user"]

    @property
    def os_group(self):
        """return OS group to use for file permissions, defined in datalogger.json"""
        return self.__config["group"]

    @property
    def cachedir(self):
        """
        return specific subdirectory to store cache files
        if this directory does not exist, it will be created

        parameters:
        datestring <str> actual datestring

        returns:
        <str> directory path
        """
        subdir = os.path.join(self.__config["cachedir"], self.datestring, self.project, self.tablename)
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        # try to set ownership of created directories
        username = self.os_user
        try:
            uid = pwd.getpwnam(username).pw_uid
            gid = pwd.getpwnam(username).pw_gid
            os.chown(os.path.join(self.__config["cachedir"], self.datestring), uid, gid)
            os.chown(os.path.join(self.__config["cachedir"], self.datestring, self.project), uid, gid)
            os.chown(os.path.join(self.__config["cachedir"], self.datestring, self.project, self.tablename), uid, gid)
        except KeyError as exc:
            logging.exception(exc)
            logging.error("User %s does not exist on this systemi, default permission will be applied to created directories", username)
        return subdir

    @property
    def interval(self):
        """return defined interval of timestamps defined in configuration"""
        return self.__meta["interval"]

    @property
    def stat_func_names(self):
        return list(TimeseriesStats.stat_funcs.keys())

    @property
    def archivepath(self):
        """path to archiving drectory for this particular setup, does not include existance of this directory"""
        return os.path.join(self.__config["archivepath"], self.datestring, self.project, self.tablename)

    @property
    def archive_filename(self):
        """filename of archive, does not include existance of file"""
        return os.path.join(self.archivepath, "%s_%s.csv.gz" % (self.tablename, self.datestring))

    def __getitem__(self, *args):
        """
        super overloaded __getitem__ function could be either
        use this method to get to plain stored data, without any sophisticated processing

        ["caches"] -> <dict>
        ["tsa"] -> return TimeseriesArray
        ["tsa", <key>] -> return Timeseries
        ["tsastats"] -> return TimeseriesArrayStats
        ["tsastats", <key>] -> return TimeseriesStats
        ["qa"] -> return QuantileArray
        ["qa", <key>] -> return <dict> Quantile
        ["total_stats"] -> return <dict> total_stats
        """
        if isinstance(args[0], str):
            kind = args[0]
            if kind == "tsa":
                try:
                    return self.__memcache_get("tsa")
                except KeyError:
                    self.__memcache_set("tsa", self.load_tsa())
                    return self.__memcache_get("tsa")
            if kind == "tsastats":
                try:
                    return self.__memcache_get("tsastats")
                except KeyError:
                    self.__memcache_set("tsastats", self.load_tsastats())
                    return self.__memcache_get("tsastats")
            if kind == "qa":
                try:
                    return self.__memcache_get("qa")
                except KeyError:
                    self.__memcache_set("qa", self.load_quantile())
                    return self.__memcache_get("qa")
            if kind == "caches":
                try:
                    return self.__memcache_get("caches")
                except KeyError:
                    self.__memcache_set("caches", self.get_caches())
                    return self.__memcache_get("caches")
            if kind == "total_stats":
                try:
                    return self.__memcache_get("total_stats")
                except KeyError:
                    self.__memcache_set("total_stats", self.load_total_stats())
                    return self.__memcache_get("total_stats")
        if isinstance(args[0], tuple):
            kind, subkey = args[0]
            if kind == "tsa":
                try:
                    tsa = self.__memcache_get("tsa")
                except KeyError:
                    tsa = self.load_tsa()
                    self.__memcache_set("tsa", tsa)
                return tsa[subkey]
            if kind == "tsastats":
                try:
                    tsastats = self.__memcache_get("tsastats")
                except KeyError:
                    tsastats = self.load_tsastats()
                    self.__memcache_set("tsastats", tsastats)
                return tsastats[subkey]
            if kind == "qa":
                try:
                    qa = self.__memcache_get("qa")
                except KeyError:
                    qa = self.load_quantile()
                    self.__memcache_set("qa", qa)
                return qa[subkey]
        else:
            raise KeyError("unknown datatype")

    def __parse_line(self, row):
        """
        specialized method to parse a single line read from raw CSV

        parameters:
        row <str> result from readline()
        timedelta <int> amount second to correct every read timestamp

        returns:
        <dict> keys from headers, all values are converted to float
        """
        data = dict(zip(self.headers, row.split(self.delimiter)))
        try:
            data[self.ts_keyname] = int(float(data[self.ts_keyname]) + self.timedelta)
        except ValueError as exc:
            logging.exception(exc)
            logging.error("ValueError on row skipping this data: %s", str(data))
        except KeyError as exc:
            logging.exception(exc)
            logging.error("KeyError on row, skipping this data: %s", str(data))
        return data

    def __get_raw_filename(self):
        """
        return filename of raw input file, if one is available
        otherwise return None
        """
        filename = os.path.join(self.raw_basedir, "%s_%s.csv" % (self.tablename, self.datestring))
        if os.path.isfile(filename):
            return filename
        else: # try .gz version
            filename += ".gz" # try gz version
            if os.path.isfile(filename):
                return filename
        # otherwise None

    def __read_raw_dict(self):
        """
        generator to return parsed lines from raw file of one specific datestring

        parameters:
        datestring <str> isodate string like 2014-12-31

        yields:
        <dict> of every row
        """
        filename = self.__get_raw_filename()
        if filename is None:
            raise AttributeError("no raw input file does exist")
        logging.debug("reading raw data from file %s", filename)
        start_ts, stop_ts = self.get_ts_for_datestring(self.__datestring) # get first and last timestamp of this date
        logging.debug("appropriate timestamps for this date are between %s and %s", start_ts, stop_ts)
        if filename.endswith(".gz"):
            filehandle = gzip.open(filename, "rt")
        else:
            filehandle = open(filename, "rt")
        with filehandle as infile:
            next(infile) # skip header line
            for lineno, row in enumerate(infile):
                if not row or row[0] == "#":
                    continue
                try:
                    data = self.__parse_line(row)
                    if self.ts_keyname not in data:
                        logging.info("Format Error in row: %s, got %s", row, data)
                        continue
                    if not start_ts <= data[self.ts_keyname] <= stop_ts:
                        logging.debug("Skipping line, ts %s not between %s and %s", data[self.ts_keyname], start_ts, stop_ts)
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

    def get_projects(self):
        """return available project, defined in datalogger.json"""
        return list(self.__config["projects"].keys())

    def get_tablenames(self, project):
        """return available tablenames for projects, defined in datalogger.json"""
        return list(self.__config["projects"][project].keys())

    def raw_reader(self):
        """
        kind of debugging method to read from raw file, like load_tsa does,
        but report every line as is, only converted into dict
        """
        for row in self.__read_raw_dict():
            yield row

    def delete_caches(self):
        """delete pre calculates caches"""
        rawfilename = self.__get_raw_filename()
        if rawfilename is not None:
            pattern_list = ("tsa_", "ts_", "tsastat_", "tsstat_", "quantile.json", "total_stats.json")
        else:
            # raw file is missing, or file is archived
            # in this case do not delete tsa file
            logging.info("original raw file is missing, tsa_ file and all ts_ files will not be deleted")
            pattern_list = ("tsastat_", "tsstat_", "quantile.json", "total_stats.json")
        # erase memcache
        self.__memcache_init()
        # erase files
        for entry in os.listdir(self.cachedir):
            absfile = os.path.join(self.cachedir, entry)
            if any((entry.startswith(pattern) for pattern in pattern_list)):
                logging.debug("deleting cached file %s", entry)
                os.unlink(absfile)

    def get_caches(self):
        """
        search for available cachefiles
        mainly to check if the raw data of this datestring is prosessed already
        pattern is mainly used only to find the correct files, more for internal use

        parameters:
        datestring <str>

        returns:
        <dict>
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
            },
            "quantile" : {
                "pattern" : "quantile.json",
                "exists" : False,
            },
            "total_stats" : {
                "pattern" : "total_stats.json",
                "exists" : False,
            }
        }
        # the original raw file could be deleted, and only the
        # calculated TSA/TSASTATS and so on are available. In this case
        # define None
        caches["tsa"]["raw"] = self.__get_raw_filename() # None if not found
        for cachetype in ("tsa", "ts", "tsastat", "tsstat"):
            file_pattern = os.path.join(self.cachedir, caches[cachetype]["pattern"])
            for abs_filename in glob.glob(file_pattern):
                filename = os.path.basename(abs_filename)
                key = self.__decode_filename(filename)
                caches[cachetype]["keys"][str(key)] = filename
        # add quantile part
        caches["quantile"]["exists"] = os.path.isfile(os.path.join(self.cachedir, "quantile.json"))
        # add total_stats part
        caches["total_stats"]["exists"] = os.path.isfile(os.path.join(self.cachedir, "total_stats.json"))
        return caches

    def generate_caches(self, use_fast=False):
        """
        meant to generate all cached files from scratch (raw input data)
        """
        logging.info("generate_caches() was called")
        if use_fast:
            logging.info("calling fast_tsa() - disabled")
            fast_tsa(self)
            logging.info("calling load_tsa()")
            tsa = self.load_tsa()
            tsa.cache = True
        else:
            logging.info("calling load_tsa_raw()")
            tsa = self.load_tsa_raw()
            logging.info("calling tsa.dump()")
            tsa.dump(self.cachedir) # save full data
            tsa.cache = True
            logging.info("calling load_tsa_finalize()")
            tsa.finalize() # convert Timeseries to Datatypes 
        logging.info("creating tsastats")
        tsastats = TimeseriesArrayStats(tsa) # calculate
        logging.info("calling tsastats.dump()")
        tsastats.dump(self.cachedir) # store
        logging.info("creating quantile_array")
        quantile_array = QuantileArray(tsa, tsastats) # caclculate
        logging.info("calling quantile_array.dump()")
        quantile_array.dump(self.cachedir) # store
        logging.info("creating total_stats()")
        total_stats = self.__caclculate_total_stats(tsastats) # calculate
        logging.info("calling total_stats.dump() - kind of")
        self.__dump_total_stats(total_stats) # store
        logging.info("done generate_caches()")
        # call this methode to refresh cached informations
        self.__memcache_init()

    def raw_to_archive(self, force=False):
        """
        method to archive raw input files to archivepath destination
        archivepath is suposed to be much slower than online,
        and maybe not always present, so it is not supposed to use it regularly

        this is only allowed if there is some existing tsa archive

        parameters:
        force <bool> to indicate if some pre-existing archived raw file should be replaced
        """
        # check raw input file existance
        raw_filename = self.__get_raw_filename()
        if raw_filename is None:
            logging.info("no raw inputfile found, skipping")
            return
        # check if tsa is generated
        if not self["caches"]["ts"]["keys"]:
            raise AttributeError("TSA Archive must exist to allow archiving of raw input data")
        # check archivepath existance
        if not os.path.isdir(self.__config["archivepath"]):
            raise AttributeError("Archive %s path does not exist", self.__meta["archivepath"])
        # generate path for this setup
        archivepath = os.path.join(self.__config["archivepath"], self.datestring, self.project, self.tablename)
        if raw_filename.endswith(".gz"):
            archive_filename = os.path.join(archivepath, os.path.basename(raw_filename))
        else:
            archive_filename = os.path.join(archivepath, os.path.basename(raw_filename) + ".gz")
        if os.path.isfile(archive_filename) and force is False:
            logging.info("archive file %s does aready exists, skipping. Use force to overwrite", archive_filename)
            return
        # create subdirs
        os.makedirs(archivepath)
        # move to destination
        if raw_filename.endswith(".gz"):
            # move if already compressed
            shutil.move(raw_filename, archive_filename)
        else:
            with open(raw_filename, "rt") as infile:
                with gzip.open(archive_filename, "wt") as outfile:
                    outfile.write(infile.read())
            # old file could be deleted
            os.unlink(raw_filename)
            # shutil.move(raw_filename, raw_filename + ".todelete")
 
    def import_tsa(self, tsa):
        """
        store tsa given in parameter in global_cache to make the data available

        usually this could be modfied existing tsa extended by some keys, or filtered or ...
        the structure has to be predefined in meta data

        the tsa can afterwards be accessed via normal frontends (web, api)

        parameters:
        tsa <TimeseriesArray> object
        """
        if self.index_keynames != tsa.index_keynames:
            raise AssertionError("provided index_keynames does not match defined index_keynames")
        if self.value_keynames != tuple(tsa.value_keynames):
            raise AssertionError("provided value_keynames does not match defined value_keynames")
        cachefilename = os.path.join(self.cachedir, TimeseriesArray.get_dumpfilename(tsa.index_keynames))
        if not os.path.isfile(cachefilename):
            tsa.dump(self.cachedir)
            tsastats = TimeseriesArrayStats(tsa)
            tsastats.dump(self.cachedir)
            qantile = QuantileArray(tsa, tsastats)
            qantile.dump(self.cachedir)
        else:
            raise Exception("TSA Archive %s exists already in cache" % cachefilename)

    def load_tsa(self, filterkeys=None, index_pattern=None):
        """
        caching version to load_tsa_raw
        if never called, get ts from load_tsa_raw, and afterwards dump_tsa
        on every consecutive call read from cached version

        parameters:
        datestring <str>
        filterkeys <tuple> or None default None
        index_pattern <str> or None default None

        HINT:
        use delete_caches to delete all precalculated files
        use setup to define some sort of timedelta to use

        returns
        <TimeseriesArray> object read from cachefile or from raw data
        """
        cachefilename = os.path.join(self.cachedir, TimeseriesArray.get_dumpfilename(self.index_keynames))
        def fallback():
            """
            fallback method to use, if reading from cache data is not possible
            """
            tsa = self.load_tsa_raw()
            tsa.dump(self.cachedir) # save full data
            # read the data afterwards to make sure there is no problem,
            # without setting tsa.cache = true this is only the tsa structure no timeseries data
            tsa = TimeseriesArray.load(self.cachedir, self.index_keynames, filterkeys=filterkeys, index_pattern=index_pattern, datatypes=self.datatypes)
            return tsa
        if not os.path.isfile(cachefilename):
            logging.info("cachefile %s does not exist, fallback read from raw data file", cachefilename)
            return fallback()
        logging.debug("loading stored TimeseriesArray object file %s", cachefilename)
        try:
            tsa = TimeseriesArray.load(self.cachedir, self.index_keynames, filterkeys=filterkeys, index_pattern=index_pattern, datatypes=self.datatypes)
            return tsa
        except IOError:
            logging.error("IOError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        except EOFError:
            logging.error("EOFError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()

    def load_tsastats(self, filterkeys=None):
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
        cachefilename = os.path.join(self.cachedir, TimeseriesArrayStats.get_dumpfilename(self.index_keynames))
        def fallback():
            """
            fallback method to use, if reading from cache data is not possible
            """
            tsa = self.load_tsa(filterkeys=None) # load full tsa, and generate statistics
            tsastats = TimeseriesArrayStats(tsa) # generate full Stats
            tsastats.dump(self.cachedir) # save it for future usage
            tsastats = TimeseriesArrayStats.load(self.cachedir, self.index_keynames, filterkeys=filterkeys) # read specific
            return tsastats
        if not os.path.isfile(cachefilename):
            logging.info("cachefile %s does not exist, fallback read from tsa archive", cachefilename)
            return fallback()
        logging.debug("loading stored TimeseriesArray object file %s", cachefilename)
        try:
            tsastats = TimeseriesArrayStats.load(self.cachedir, self.index_keynames, filterkeys=filterkeys)
            return tsastats
        except IOError:
            logging.error("IOError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()
        except EOFError:
            logging.error("EOFError while reading from %s, using fallback", cachefilename)
            os.unlink(cachefilename)
            return fallback()

    def load_quantile(self):
        """
        retuns quantile for this specific tsa, either load cache version,
        or recreate from tsa

        parameters:
        datestring <str>

        returns:
        <QuantileArray>
        """
        cachefilename = QuantileArray.get_dumpfilename(self.cachedir)
        quantile_array = None
        if os.path.isfile(cachefilename):
            quantile_array = QuantileArray.load(self.cachedir)
        else:
            logging.info("cachefile %s does not exist, fallback read from tsa archive", cachefilename)
            tsa = self["tsa"]
            tsa.cache = True # to enable in memory caching of timeseries
            # huge performance improvement, from 500s to 70s
            tsastats = self["tsastats"]
            quantile_array = QuantileArray(tsa, tsastats)
            quantile_array.dump(self.cachedir)
        return quantile_array
    
    def __caclculate_total_stats(self, tsastats):
        aggregator = {
            'median': lambda a, b: 0.0, # median of medians
            'avg': lambda a, b: a + b,
            'last': lambda a, b: 0.0,
            'diff': lambda a, b: 0.0,
            'max': lambda a, b: max(a, b),
            'first': lambda a, b: 0.0,
            'min': lambda a, b: min(a, b),
            'std': lambda a, b: 0.0,
            'count': lambda a, b: a + b,
            'mean': lambda a, b: 0.0,
            'dec': lambda a, b: a + b,
            'inc': lambda a, b: a + b,
            'sum': lambda a, b: a + b,
            'total_count' : lambda a, b: a # to be consistent
        }
        stats_data = {}
        for value_keyname in self.value_keynames:
            stats_data[value_keyname] = dict((key, 0.0) for key in aggregator.keys()) # prefill with 0.0
            for index_key in tsastats.keys():
                stats = tsastats[index_key]
                for stat_func in stats[value_keyname].keys():
                    stats_data[value_keyname][stat_func] = aggregator[stat_func](stats_data[value_keyname][stat_func], stats[value_keyname][stat_func])
                stats_data[value_keyname]["total_count"] += 1
            if stats_data[value_keyname]["total_count"] > 0:
                stats_data[value_keyname]["total_avg"] = stats_data[value_keyname]["sum"] / stats_data[value_keyname]["total_count"]
                stats_data[value_keyname]["avg"] /= stats_data[value_keyname]["total_count"]
            else:
                stats_data[value_keyname]["total_avg"] = 0.0
                stats_data[value_keyname]["avg"] = 0.0
        return stats_data

    def __dump_total_stats(self, total_stats):
        """
        dump total_stats to file
        """
        cachefilename = os.path.join(self.cachedir, "total_stats.json")
        with open(cachefilename, "wt") as outfile:
            json.dump(total_stats, outfile, indent=4)
 
    def __load_total_stats(self):
        """
        load total_stats from file
        """
        cachefilename = os.path.join(self.cachedir, "total_stats.json")
        with open(cachefilename, "rt") as infile:
            total_stats = json.load(infile)
        return total_stats
 
    def load_total_stats(self):
        """
        aggregates all TimeseriesStats available in TimeseriesArrayStats to total_stats dict

        returns:
        <dict> of statistical functions, and values
        """
        cachefilename = os.path.join(self.cachedir, "total_stats.json")
        if not os.path.isfile(cachefilename):
            tsastats = self["tsastats"]
            total_stats = self.__caclculate_total_stats(tsastats)
            self.__dump_total_stats(total_stats)
            return total_stats
        else:
            total_stats = self.__load_total_stats()
            return total_stats

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
            try:
                key = b64eval(key_encoded)
                assert isinstance(key, tuple)
                return key
            except Exception as exc:
                logging.exception(exc)
                raise DataLoggerFilenameDecodeError("filename %s could not be decoded to tuple, result: %s" % (filename, key))
        except Exception as exc:
            logging.exception(exc)
            raise DataLoggerFilenameDecodeError("Something went wrong while decoding filensme %s" % filename)

    def load_tsa_raw(self):
        """
        read data from raw input files and return TimeseriesArray object

        parameters:
        datestring <str> isodate representation of date like 2015-12-31
        timedelta <int> amount second to correct raw input timestamps

        returns:
        <TimeseriesArray> object wich holds all data of this day
        """
        tsa = TimeseriesArray(self.index_keynames, self.value_keynames, datatypes=self.datatypes)
        for rowdict in self.__read_raw_dict():
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
        function to walf from first day to last day in given month
        """
        year, month = monthdatestring.split("-")
        lastday = calendar.monthrange(int(year), int(month))[1]
        start = "%04d-%02d-01" % (int(year), int(month))
        stop = "%04d-%02d-%02d" % (int(year), int(month), lastday)
        return DataLogger.datewalker(start, stop)

    def get_tsastats_longtime_hc(self, monthstring, key, value_key):
        """
        TODO: do this in webapp, not here, too special
        method to get longtime data from stored TimeseriesArrayStats objects
        and return data usable as higcharts input
        """
        # datalogger = DataLogger(BASEDIR, project, tablename)
        filterkeys = dict(zip(self.index_keynames, key))
        logging.debug("build filterkeys %s", filterkeys)
        ret_data = {}
        for datestring in self.monthwalker(monthstring):
            logging.debug("getting tsatstats for %s", monthstring)
            try:
                tsastats = self.load_tsastats(filterkeys)
                for funcname in tsastats[key][value_key].keys():
                    if funcname in ret_data:
                        ret_data[funcname].append((datestring, tsastats[key][value_key][funcname]))
                    else:
                        ret_data[funcname] = [(datestring, tsastats[key][value_key][funcname]), ]
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
