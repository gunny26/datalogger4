#!/usr/bin/python3
"""
Datalogger Core functionality to get to the data

special functions should be placed in different modules
To get some special formats for HighCharts or DataTable there will be special modules.

Design considerations:
    * every call will return json message
    * only real server faults will result in 500
    * use status code to indicate failure
        200 - OK
        400 - bad request - something is wrong with request
        401 - unauthorized - something in request is missing to authorize
        403 - forbidden - you are not allowed
        404 - not found
        406 - not aceptable - one or more parameters are not correct
    * check for X-APIKEY to authorized scripts
    * check for X-AUTH-KEY to authorized JS Applications with User authentication
    * keep it simple, do one thing only
    * use central config in yaml format
 
"""
import os
import json
import gzip
import datetime
import time
import re
import logging
logging.basicConfig(level=logging.INFO)
# non stdlib
# import yaml
from flask import Flask, Response, request, abort, jsonify
from flask_cors import CORS
# own modules
from flask_tk.TkWebConfig import TkWebConfig # main config
from flask_tk.TkFlaskDecorators import xapikey, jsonout # decorators
from datalogger3.CustomExceptions import DataLoggerLiveDataError
from datalogger3.DataLogger import DataLogger

logger = logging.getLogger("DataLoggerFlask")
CONFIG = TkWebConfig("/var/www/DataLoggerWebApp.yaml")
BASEDIR = CONFIG["custom"]["basedir"]
_dl = DataLogger(BASEDIR)
app = Flask(__name__)
CORS(app) # enable CORS for all methods
application = app # WSGI Module will call application.run()

@app.route("/")
@xapikey(CONFIG)
@jsonout
def index():
    """return main config"""
    return "DataLoggerFlask v 1.0"

@app.route("/ts_for_datestring/<datestring>")
@xapikey(CONFIG)
@jsonout
def get_ts_for_datestring(datestring):
    """return fist an last timestamp for this datestring"""
    first_ts, last_ts = _dl.get_ts_for_datestring(datestring)
    return {"first_ts" : first_ts, "last_ts": last_ts}

@app.route("/stat_func_names")
@xapikey(CONFIG)
@jsonout
def get_stat_func_names():
    """return all statistical function names"""
    return _dl.stat_func_names

@app.route("/yesterday_datestring")
@xapikey(CONFIG)
@jsonout
def get_yesterday_datestring():
    """return datestring of yesterday"""
    return _dl.get_yesterday_datestring()

@app.route("/last_business_day_datestring")
@xapikey(CONFIG)
@jsonout
def get_last_business_day_datestring():
    """return datestring of last businessday"""
    return _dl.get_last_business_day_datestring()

@app.route("/projects", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_projects():
    """return all available projects"""
    return _dl.get_projects()

@app.route("/tablenames/<project>", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_tablenames(project):
    """return all available tablenames for this project"""
    return _dl.get_tablenames(project)

@app.route("/desc/<project>/<tablename>", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_desc(project, tablename):
    """return structure of tablename in project"""
    _dl.setup(project, tablename, "1970-01-01")
    return _dl.meta

@app.route("/caches/<project>/<tablename>/<datestring>", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_caches(project, tablename, datestring):
    """return structure of tablename in project"""
    _dl.setup(project, tablename, datestring)
    return _dl["caches"]

@app.route("/index_keys/<project>/<tablename>/<datestring>", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_index_keys(project, tablename, datestring):
    """return all available index_keys for this data"""
    _dl.setup(project, tablename, datestring)
    caches = _dl["caches"]
    return list(caches["tsa"]["keys"].keys())

@app.route("/tsa/<project>/<tablename>/<datestring>", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_tsa(project, tablename, datestring):
    """return TimeseriesArray Structure"""
    _dl.setup(project, tablename, datestring)
    return _dl["tsa"].to_data()

@app.route("/tsastats/<project>/<tablename>/<datestring>", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_tsastats(project, tablename, datestring):
    """return TimeseriesArrayStats Structure"""
    _dl.setup(project, tablename, datestring)
    return _dl["tsastats"].to_data()

@app.route("/quantile/<project>/<tablename>/<datestring>", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_quantile(project, tablename, datestring):
    """return Quantile structure"""
    _dl.setup(project, tablename, datestring)
    return _dl["qa"].to_data()

@app.route("/total_stats/<project>/<tablename>/<datestring>")
@xapikey(CONFIG)
@jsonout
def get_total_stats(project, tablename, datestring):
    """return total stats for this project/tablename/datestring"""
    _dl.setup(project, tablename, datestring)
    return _dl["total_stats"]

@app.route("/ts", methods=["GET"])
@xapikey(CONFIG)
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

@app.route("/tsstats", methods=["GET"])
@xapikey(CONFIG)
@jsonout
def get_tsstats():
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
@xapikey(CONFIG)
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
    _dl = DataLogger(BASEDIR)
    logger.info("project %s created", project)
    return "project created"

@app.route("/tablename/<project>/<tablename>", methods=["POST"])
@xapikey(CONFIG)
@jsonout
def post_tablename(project, tablename):
    """
    will create new tablename in project
    table definition must be provided in data segment of body
    request must be application/json

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
    with open(metafile, "wt") as outfile:
        json.dump(data["meta"], outfile, indent=4)
        # reinit DataLogger object
        _dl = DataLogger(BASEDIR)
    return "table %s created" % tablename

@app.route("/raw_file/<project>/<tablename>/<datestring>", methods=["POST"])
@xapikey(CONFIG)
@jsonout
def post_raw_file(project, tablename, datestring):
    """
    save received data for whole day datafile
    file must not exist in prior

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
    # reread saved data
    try:
        tsa = _dl["tsa"] # re-read received data
    except AssertionError as exc:
        logger.exception(exc)
        os.unlink(filename)
        logger.info("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
        return "Invalid data in uploaded file, see apache error log for details, uploaded file not stored"
    logger.info("File stored")
    return "File stored"

@app.route("/append/<project>/<tablename>", methods=["PUT"])
@xapikey(CONFIG)
@jsonout
def put_append(project, tablename):
    """
    appending some data to actual live raw datafile
    request contains only one single line of data

    TODO: quality check and return codes
    """
    datestring = datetime.date.today().isoformat() # today
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat() # used to initialize datalogger
    _dl.setup(project, tablename, yesterday) # TODO: initialization could only be done for datestring in the past
    row = request.get_json() # get query as dict, use input not data
    ts = time.time() # timestamp data was received
    valid, message, status_code = _row_is_valid(row, ts) # check validity
    if valid is False:
        return message, status_code
    filename = os.path.join(_dl.raw_basedir, "%s_%s.csv" % (tablename, datestring))
    firstline = False # inidcate headerline or not
    if not os.path.isfile(filename):
        fh = open(filename, "wt")
        firstline = True
    else:
        fh = open(filename, "at")
    with fh as outfile:
        # actually write
        if firstline is True:
            logger.info("raw file does not exist, will create new file %s", filename)
            outfile.write("\t".join(_dl.headers) + "\n") # TODO: use delimiter defined in meta
            firstline = False
        logger.info("raw file %s does already exist, appending", filename)
        outfile.write("\t".join([str(row[key]) for key in _dl.headers]) + "\n")
    return "data appended", 200

@app.route("/appendmany/<project>/<tablename>", methods=["PUT"])
@xapikey(CONFIG)
@jsonout
def put_appendmany(project, tablename):
    """
    appening many data rows to some project/tablename -s actual live data
    for historical data use post_raw_file
    request contains many lines of data

    TODO: quality check and return codes, and improvements
    """
    datestring = datetime.date.today().isoformat()
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    _dl.setup(project, tablename, yesterday) # TODO: initialize with yesterday, today is not allowed
    data = request.get_json() # will raise BadRequest if not json formatted
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
                outfile.write("\t".join(_dl.headers) + "\n") # TODO: use delimiter defined
                firstline = False
            logger.info("raw file %s does already exist, appending", filename)
            outfile.write("\t".join([str(row[key]) for key in _dl.headers]) + "\n")
    return "%d rows appended" % len(rows)

@app.route("/caches/<project>/<tablename>/<datestring>", methods=["DELETE"])
@xapikey(CONFIG)
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
    app.run(host="0.0.0.0") # when started as program
