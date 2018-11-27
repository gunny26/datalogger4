#!/usr/bin/python3
"""
Datalogger Core functionality to get to the data

special functions should be placed in different modules
"""
import os
import json
import gzip
import datetime
import time
import re
import logging
logging.basicConfig(level=logging.INFO)
from flask import Flask, Response, request, abort, jsonify
# own modules
#import tk_web
from datalogger3.CustomExceptions import DataLoggerLiveDataError
from datalogger3.DataLogger import DataLogger as DataLogger


def xapikey(config):
    def _xapikey(func):
        """
        decorator to check for existance and validity of X-APIKEY header
        """
        def __xapikey(*args, **kwds):
            if request.remote_addr in config["REMOTE_ADDRS"]:
                app.logger.error("call from trusted client %s", request.remote_addr)
                return func(*args, **kwds)
            x_token = request.headers.get("x-apikey")
            if not x_token:
                app.logger.error("X-APIKEY header not provided")
                return "wrong usage", 401
            if x_token not in config["APIKEYS"]:
                app.logger.error("X-APIKEY is unknown")
                abort(403)
            if config["APIKEYS"][x_token]["remote_addrs"] and request.remote_addr not in config["APIKEYS"][x_token]["remote_addrs"]:
                app.logger.error("call from %s with %s not allowed", request.remote_addr, x_token)
                abort(403)
            app.logger.info("authorized call from %s with %s", request.remote_addr, x_token)
            return func(*args, **kwds)
        __xapikey.__name__ = func.__name__ # crucial setting to not confuse flask
        __xapikey.__doc__ = func.__doc__ # crucial setting to not confuse flask
        return __xapikey
    return _xapikey

def jsonout(func):
    """
    decorator to format response in json format
    """
    def _jsonout(*args, **kwds):
        data = None # default value
        try:
            ret = func(*args, **kwds)
            if isinstance(ret, tuple):
                if len(ret) == 3:
                    message, status_code, data = ret
                elif len(ret) == 2:
                    message, status_code = ret
                    data = None
                elif len(ret) == 1:
                    message = ret[0]
                    status_code = 200
                    data = message
            message = "call to %s done" % func.__name__
            status_code = 200
            data = ret
        except (KeyError, IndexError) as exc:
            logger.error(exc)
            status_code = 404
            message = str(exc)
        except (IOError, OSError) as exc:
            logger.error(exc)
            status_code = 500
            message = str(exc)
        except AttributeError as exc:
            logger.error(exc)
            status_code = 406
            message = str(exc)
        return jsonify({
            "message" : message,
            "status_code" : status_code,
            "data" : data
        })
    _jsonout.__name__ = func.__name__ # crucial setting to not confuse flask
    _jsonout.__doc__ = func.__doc__ # crucial setting to not confuse flask
    return _jsonout

CONFIG = json.load(open("/var/www/DataLoggerWebApp.json", "rt"))
_dl = DataLogger(CONFIG["BASEDIR"])
logger = logging.getLogger("DataLoggerFlask")
app = Flask(__name__)
application = app # WSGI Module will call application.run()

@app.route("/")
@xapikey(CONFIG)
@jsonout
def index():
    """return main config"""
    return "DataLoggerFask v 0.1"

@app.route("/ts_for_datestring/<datestring>")
@jsonout
def get_ts_for_datestring(datestring):
    first_ts, last_ts = _dl.get_ts_for_datestring(datestring)
    return {"first_ts" : first_ts, "last_ts": last_ts}

@app.route("/stat_func_names")
@jsonout
def get_stat_func_names():
    return _dl.stat_func_names

@app.route("/yesterday_datestring")
@jsonout
def get_yesterday_datestring():
    return _dl.get_yesterday_datestring()

@app.route("/last_business_day_datestring")
@jsonout
def get_last_business_day_datestring():
    return _dl.get_last_business_day_datestring()

@app.route("/projects", methods=["GET"])
@jsonout
def get_projects():
    """return all available projects"""
    return _dl.get_projects()

@app.route("/tablenames/<project>", methods=["GET"])
@jsonout
def get_tablenames(project):
    """return all available tablenames for this project"""
    return _dl.get_tablenames(project)

@app.route("/desc/<project>/<tablename>", methods=["GET"])
@jsonout
def get_desc(project, tablename):
    """return structure of specified tablename"""
    _dl.setup(project, tablename, "1970-01-01")
    return _dl.meta

@app.route("/index_keys/<project>/<tablename>/<datestring>", methods=["GET"])
@jsonout
def get_index_keys(project, tablename, datestring):
    """return all available index_keys"""
    _dl.setup(project, tablename, datestring)
    caches = _dl["caches"]
    return list(caches["tsa"]["keys"].keys())

@app.route("/tsa/<project>/<tablename>/<datestring>", methods=["GET"])
@jsonout
def get_tsa(project, tablename, datestring):
    """return Timeseries, filter in data
    """
    _dl.setup(project, tablename, datestring)
    return _dl["tsa"].to_data()

@app.route("/tsastat/<project>/<tablename>/<datestring>", methods=["GET"])
@jsonout
def get_tsastat(project, tablename, datestring):
    """return Timeseries, filter in data
    """
    _dl.setup(project, tablename, datestring)
    return _dl["tsastats"].to_data()

@app.route("/quantile/<project>/<tablename>/<datestring>", methods=["GET"])
@jsonout
def get_quantile(project, tablename, datestring):
    """return Timeseries, filter in data
    """
    _dl.setup(project, tablename, datestring)
    return _dl["qa"].to_data()

@app.route("/total_stats/<project>/<tablename>/<datestring>")
@jsonout
def get_total_stats(project, tablename, datestring):
    """ using DataLogger method """
    _dl.setup(project, tablename, datestring)
    return _dl["total_stats"]

@app.route("/ts", methods=["GET"])
@jsonout
def get_ts():
    """
    return Timeseries, filter in data
    datestring = required
    index_key = required
    value_keyname = optional
    """
    data = request.get_json()
    # build data
    result = {}
    _dl.setup(data["project"], data["tablename"], data["datestring"])
    for index_key in _get_index_keys(data):
        index_key = tuple(index_key)
        if data["value_keynames"]:
            result[str(index_key)] = list(_dl["tsa", index_key].to_data(data["value_keynames"]))
        else:
            result[str(index_key)] = list(_dl["tsa", index_key].to_data())
    return "returned %d timeseries" % len(result), 200, result

@app.route("/tsstat", methods=["GET"])
@jsonout
def get_tsstat():
    """return Timeseries, filter in data
    datestring = required
    index_key = required
    value_keyname = optional
    """
    data = request.get_json()
    _dl.setup(data["project"], data["tablename"], data["datestring"])
    # build data
    result = {}
    for index_key in _get_index_keys(data):
        index_key = tuple(index_key)
        result[str(index_key)] = _dl["tsastats", index_key].to_data()
    return "returned %d timeseries" % len(result), 200, result

@app.route("/project/<project>", methods=["POST"])
@jsonout
def post_project(project):
    """
    will create new project
    assure the project does not yet exist

    returns:
    200 - if OK
    406 - if format not OK
    409 - if something does already exist
    """
    # check string
    # must be lowercase only
    # consisting of a-z and 1-9
    # no other characters allowed
    # max. length 12
    if re.match("^[a-z0-9]{0,12}$", project) is None:
        logger.error("project %s does not match formatting criterias", project)
        return "format mismatch", 406
    if os.path.isdir(os.path.join(_dl.basedir, project)):
        logger.error("project %s subdir does already exist", project)
        return "project subdir already exists", 409
    if project in _dl.get_projects():
        logger.error("project %s does already exist", project)
        return "project already exists", 409
    os.mkdir(os.path.join(_dl.basedir, project))
    os.mkdir(os.path.join(_dl.basedir, project, "meta"))
    os.mkdir(os.path.join(_dl.basedir, project, "raw"))
    # reinit DataLogger object
    _dl = DataLogger(CONFIG["BASEDIR"])
    logger.info("project %s created", project)
    return "project created"

@app.route("/tablename/<project>/<tablename>", methods=["POST"])
@jsonout
def post_tablename(project, tablename):
    """
    will create new project
    assure the project does not yet exist

    returns
    200 - if all ok
    406 - if format is not OK
    409 - if something already exists
    """
    # check string
    # must be lowercase only
    # consisting of a-z and 1-9
    # no other characters allowed
    # max. length 32
    data = request.get_json()
    if re.match("^[a-zA-Z0-9]{0,32}$", tablename) is None:
        logger.error("tablename %s does not match formatting criterias", tablename)
        return "format mismatch", 406
    metafile = os.path.join(_dl.basedir, project, "meta", "%s.json" % tablename)
    if os.path.isfile(metafile):
        logger.error("table definition file for tablename %s does already exist", tablename)
        return "table definition file already exists", 409
    meta = data["meta"] # get query as dict, use input not data
    with open(metafile, "wt") as outfile:
        json.dump(meta, outfile, indent=4)
        # reinit DataLogger object
        _dl = DataLogger(CONFIG["BASEDIR"])
    return "table %s created" % tablename

@app.route("/raw_file/<project>/<tablename>/<datestring>", methods=["POST"])
@jsonout
def post_raw_file(project, tablename, datestring):
    """
    save receiving file into datalogger structure

    /project/tablename/datestring
    """
    _dl.setup(project, tablename, datestring)
    filename = os.path.join(_dl.raw_basedir, "%s_%s.csv.gz" % (tablename, datestring))
    if os.path.isfile(filename):
        logger.info("File already exists")
        return "File already exists", 409
    try:
        with gzip.open(filename, "wt") as outfile:
            x = web.input(myfile={})
            logger.info(x.keys())
            logger.info("Storing data to %s", filename)
            if "filedata" in x: # curl type
                outfile.write(x["filedata"])
            else: # requests or urllib3 type
                outfile.write(x["myfile"].file.read())
    except Exception as exc:
        logger.exception(exc)
        os.unlink(filename)
        logger.info("Error while saving received data to")
        return "Error while saving received data to file", 500
    try:
        tsa = _dl["tsa"] # re-read received data
    except AssertionError as exc:
        logger.exception(exc)
        os.unlink(filename)
        logger.info("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
        return "Invalid data in uploaded file, see apache error log for details, uploaded file not stored"
    logger.info("File stored")
    return "File stored"

@app.route("/append/<project>/<tablename>/<datestring>", methods=["PUT"])
@jsonout
def put_append(project, tablename, datestring):
    """
    appening some data to actual live raw datafile
    TODO: quality check and return codes
    """
    datestring = datetime.date.today().isoformat()
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    _dl.setup(project, tablename, yesterday) # TODO: initialize with yesterday, today is not allowed
    row = request.get_json() # get query as dict, use input not data
    ts = time.time()
    valid, message, status_code = _row_is_valid(row, ts)
    if valid is False:
        return message, status_code
    filename = os.path.join(_dl.raw_basedir, "%s_%s.csv" % (tablename, datestring))
    firstline = False
    if not os.path.isfile(filename):
        fh = open(filename, "wt")
        firstline = True
    else:
        fh = open(filename, "at")
    with fh as outfile:
        # actually write
        if firstline is True:
            logger.info("raw file does not exist, will create new file %s", filename)
            outfile.write("\t".join(_dl.headers) + "\n")
            firstline = False
        logger.info("raw file %s does already exist, appending", filename)
        outfile.write("\t".join([str(row[key]) for key in _dl.headers]) + "\n")
    return "data appended", 200

@app.route("/appendmany/<project>/<tablename>/<datestring>", methods=["PUT"])
@jsonout
def put_appendmany(project, tablename, datestring):
    """
    appening many data rows to some project/tablename -s actual live data
    for historical data use post_raw_file

    TODO: quality check and return codes, and improvements
    """
    datestring = datetime.date.today().isoformat()
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    _dl.setup(project, tablename, yesterday) # TODO: initialize with yesterday, today is not allowed
    data = request.get_json()
    ts = time.time()
    rows = data["rows"] # TODO: has to be list json formatted
    filename = os.path.join(_dl.raw_basedir, "%s_%s.csv" % (tablename, datestring))
    firstline = False
    if not os.path.isfile(filename):
        fh = open(filename, "wt")
        firstline = True
    else:
        fh = open(filename, "at")
    with fh as outfile:
        for row in rows:
            valid, message, status_code = _row_is_valid(row, ts)
            if valid is False:
                return message, status_code
            # actually write
            if firstline is True:
                logger.info("raw file does not exist, will create new file %s", filename)
                outfile.write("\t".join(_dl.headers) + "\n")
                firstline = False
            logger.info("raw file %s does already exist, appending", filename)
            outfile.write("\t".join([str(row[key]) for key in _dl.headers]) + "\n")
    return "%d rows appended" % len(rows)

@app.route("/caches/<project>/<tablename>/<datestring>")
@jsonout
def delete_caches(project, tablename, datestring):
    """
    delete all available caches for this specific entry
    """
    _dl.setup(project, tablename, datestring)
    _dl.delete_caches()


############### private functions ##################################

def _get_index_keys(data):
    """
    return list of possible index_keys to fetch data from
    if index_key in data this will be used to match exactly
    if index_pattern is given, all matches will be returned
    """
    _dl.setup(data["project"], data["tablename"], data["datestring"])
    index_keys = []
    if data["index_key"]: # use only one
        if tuple(data["index_key"]) in _dl["tsa"].keys():
            index_keys.append(data["index_key"])
    else: # search by pattern in available index_keys
        index_keys = []
        index_pattern = data["index_pattern"]
        for index_key in _dl["tsa"].keys():
            index_key_dict = dict(zip(_dl.index_keynames, index_key))
            if all((index_pattern[key].lower() in index_key_dict[key].lower() for key in index_pattern.keys() if index_pattern[key])):
                index_keys.append(index_key)
    return index_keys

def _row_is_valid(row, ts):
    """
    return True if row is valid, otherwise False
    ts data was received
    """
    # self.logger.info("received data to store %s", row)
    if not all((index_key in row for index_key in _dl.index_keynames)):
        logger.error("some index_key is missing")
        return False, "some index_key is missing", 409
    if not all((value_key in row for value_key in _dl.value_keynames)):
        logger.error("some value_key is missing")
        return False, "some value_key is missing", 409
    if not _dl.ts_keyname in row:
        logger.error("ts_key is missing")
        return False, "ts_key is missing", 409
    # check timestamp
    min_ts = ts - 60
    max_ts = ts + 60
    if not min_ts < float(row[_dl.ts_keyname]) < max_ts:
        logger.info("timestamp in received data is out of range +/- 60s")
        return False, "timestamp in received data is out of range +/- 60s", 409
    return True, "row is valid", 200
    


if __name__ == "__main__":
    app.run(host="0.0.0.0")
