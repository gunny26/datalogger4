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
from functools import wraps
from inspect import isfunction
# non stdlib
import yaml
from flask import Flask, url_for, Response, request, jsonify
# from flask_cors import CORS
from werkzeug.contrib.cache import FileSystemCache, SimpleCache
# own modules
from datalogger4 import DataLogger, DataLoggerLiveDataError, b64eval, b64encode, b64decode

# this must be placed at TOP
app = Flask(__name__)
# CORS(app) # enable CORS
#_fs_cache = FileSystemCache("/web/rest-apis.tirol-kliniken.cc/datalogger/v4/.cache", threshold=20000, default_timeout=0)
#_s_cache = SimpleCache(threshold=1000, default_timeout=5*60)
# include some commons sourceode
# import does not work in flask with global app ...
#exec(open("/web/rest-apis.tirol-kliniken.cc/commons.py").read())

# also on TOP to use it further down
def apihandler(func):
    @wraps(func)
    def decorated_function(*args, **kwds):
        try:
            starttime = time.time()
            ret = func(*args, **kwds)
            return jsonify({
                "duration": time.time() - starttime,
                "results": ret,
                "timestamp" : time.time()
            })
        except (KeyError, IndexError) as exc:
            logger.error(exc)
            return jsonify({"error": str(exc), "status_code": 404})
        except AttributeError as exc:
            logger.error(exc)
            return jsonify({"error": str(exc), "status_code": 400})
        except Exception as exc:
            logger.exception(exc)
            return jsonify({"error": str(exc), "status_code": 500})
    return decorated_function

# use this decorator to fine tune cache-control setting for some endpoints
def cache_control(*args, **kwds):
    """
    using first positional argument arsg[0] to set in Cache-Control Header
    if not present set to no-store
    """
    if not args:
        value = "no-store"
    else:
        value = args[0]
    def outer(func):
        @wraps(func)
        def decorated_function(*args, **kwds):
            resp = func(*args, **kwds) # should return response
            resp.headers["Cache-Control"] = value
            return resp
        return decorated_function
    return outer

# use this decorator to fine tune cache-control setting for some endpoints
def dummy(*args, **kwds):
    def outer(func):
        @wraps(func)
        def decorated_function(*args, **kwds):
            resp = func(*args, **kwds) # should return response
            return resp
        return decorated_function
    return outer
fs_cache = dummy

@app.route("/doc", methods=["GET"])
def get_doc():
    """
    summary: return swagger-ui yaml
    responses:
      200:
        description: yaml definition
        content:
          application/yaml:
            schema:
              type: object
    """
    return create_swagger_json()

@app.route("/hello", methods=["GET"])
@apihandler
def get_hello():
    """
    summary: return string hello
    """
    return "hello"

######################## OK custom API begins below ##############################

@app.route("/ts_for_datestring/<datestring>")
@cache_control("public, max-age=31536000")
@apihandler
def get_ts_for_datestring(datestring):
    """
    summary: return first and last unix timestamp of this datestring
    parameters:
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: example /ts_for_datestring/2019-08-01
    """
    first_ts, last_ts = _dl.get_ts_for_datestring(datestring)
    return {"first_ts" : first_ts, "last_ts": last_ts}

@app.route("/yesterday_datestring")
@cache_control("no-store")
@apihandler
def get_yesterday_datestring():
    """
    summary: return datestring of yesterday, usually this is the newest dataset available in datalogger
    parameters:
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: example /ts_for_datestring/2019-08-01
    """
    return _dl.get_yesterday_datestring()

@app.route("/last_business_day_datestring")
@cache_control("no-store")
@apihandler
def get_last_business_day_datestring():
    """
    summary: return datestring of last businessday (MO-FR)
    """
    return _dl.get_last_business_day_datestring()

@app.route("/datestrings")
@cache_control("public, max-age=300")
@apihandler
def get_datestrings():
    """
    summary: return list of datestring any data is stored for all projects/tablenames
    description: "this function indicates f some sort of data for the datestrings returned is available,
        make sure, if for your particular project/tablename this data also exists."
    """
    return list((datestring for datestring in sorted(os.listdir(_dl.global_cachedir)) if len(datestring) == 10 and len(datestring.split("-")) == 3))

@app.route("/stat_func_names")
@cache_control("public, max-age=31536000")
@apihandler
def get_stat_func_names():
    """
    summary: return available statistical functions used in datalogger
    """
    return _dl.stat_func_names

@app.route("/projects", methods=["GET"])
@cache_control("public, max-age=86400")
@apihandler
def get_projects():
    """
    summary: return all configured projects in datalogger
    """
    return _dl.get_projects()

@app.route("/tablenames/<project>", methods=["GET"])
@cache_control("public, max-age=86400")
@apihandler
def get_tablenames(project):
    """
    summary: return all configured tablenames for project
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
    description: example /tablenames/cmdb
    """
    return _dl.get_tablenames(project)

@app.route("/desc/<project>/<tablename>", methods=["GET"])
@cache_control("public, max-age=86400")
@apihandler
def get_desc(project, tablename):
    """
    summary: return structure of table in project
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
    description: example /desc/cmdb/vicenterVms
    """
    _dl.setup(project, tablename, "1970-01-01")
    return _dl.meta

@app.route("/exists/<project>/<tablename>/<datestring>", methods=["GET"])
@cache_control("no-store")
@apihandler
def get_exists(project, tablename, datestring):
    """
    summary: return if analyzed data is available
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: "example /exists/glt/energiemonitor/2019-09-01
        endpoint to quick check if some data is available before calling specialized functions"
    """
    total_stats_filename = os.path.join(_dl.global_cachedir, datestring, project, tablename, "total_stats.json")
    if os.path.isfile(total_stats_filename):
        return {"exists" : True}
    return {"exists": False}

@app.route("/index/<project>/<tablename>/<datestring>", methods=["GET"])
@cache_control("public, max-age=31536000")
@apihandler
def get_index(project, tablename, datestring):
    """
    summary: information about stored data of tablename in project on datestring
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: "example /index/cmdb/vicenterVms/2019-08-01
        this endpoint is useful to find the stored index_keys in either base64 or str representation"
    """
    return list(_get_table_index(project, tablename, datestring))

@app.route("/search/<datestring>/<pattern>", methods=["GET"])
@cache_control("no-store")
@apihandler
def get_search(datestring, pattern):
    """
    summary: search for some indizes on datestring using pattern in all projects/tablenames
    parameters:
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
      - name: pattern
        in: path
        required: true
        description: portion of string to search for, at least 3 characters long
        schema:
          type: string
    description: example /search/2019-08-01/srvmghomer
    """
    ret_data = []
    if len(pattern) < 4:
        raise AttributerError("pattern must be at least 3 charcaters long")
    for project in list(_dl.get_projects()):
        for tablename in list(_dl.get_tablenames(project)):
            for entry in _get_table_index(project, tablename, datestring):
                if pattern in entry["_str_key"]:
                    ret_data.append(entry)
    return ret_data

@app.route("/stats_by_value_keyname/<project>/<tablename>/<datestring>/<value_keyname>", methods=["GET"])
@cache_control("public, max-age=86400")
@apihandler
def get_stats_by_value_keyname(project, tablename, datestring, value_keyname):
    """
    summary: return daily statistics for one particular value_keyname, with ever statistical function available
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: "example /datalogger/v4/stats_by_value_keyname/vicenter/virtualMachineMemoryStats/2019-10-28/mem.active.average"
    """
    _dl.setup(project, tablename, datestring)
    tsastats = _dl["tsastats"]
    ret_data = []
    for index_key, stats in tsastats.stats.items():
        row_data = {
            "_project": project,
            "_tablename": tablename,
            "_datestring": datestring,
            "_key": dict(zip(_dl.meta["index_keynames"], index_key)),
            "_str_key": str(index_key),
            "_b64_key": b64encode(index_key),
            "_value_keyname": value_keyname,
        }
        for stat_func_name in _dl.stat_func_names:
            row_data[stat_func_name] = stats[value_keyname][stat_func_name]
        ret_data.append(row_data)
    return ret_data

@app.route("/stats_by_func/<project>/<tablename>/<datestring>/<stat_func_name>", methods=["GET"])
@cache_control("public, max-age=86400")
@apihandler
def get_stats_by_func(project, tablename, datestring, stat_func_name):
    """
    summary: return daily statistics for one particular statistical function for every value_keyname in table
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
      - name: stat_func_name
        in: path
        required: true
        description: one of /stat_func_names
        schema:
          type: string
    description: "example /datalogger/v4/stats_by_func/vicenter/virtualMachineMemoryStats/2019-10-28/sum"
    """
    _dl.setup(project, tablename, datestring)
    tsastats = _dl["tsastats"]
    ret_data = []
    for index_key, stats in tsastats.stats.items():
        row_data = {
            "_project": project,
            "_tablename": tablename,
            "_datestring": datestring,
            "_key": dict(zip(_dl.meta["index_keynames"], index_key)),
            "_str_key": str(index_key),
            "_b64_key": b64encode(index_key),
            "_stat_func_name": stat_func_name,
        }
        for value_keyname in _dl.value_keynames:
            row_data[value_keyname] = stats[value_keyname][stat_func_name]
        ret_data.append(row_data)
    return ret_data

@app.route("/quantile/<project>/<tablename>/<datestring>", methods=["GET"])
@cache_control("public, max-age=31536000")
@apihandler
def get_quantile(project, tablename, datestring):
    """
    summary: return Quantile data
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: example /quantile/cmdb/vicenterVms/2019-08-01
    """
    _dl.setup(project, tablename, datestring)
    return _dl["qa"].to_data()

@app.route("/total_stats/<project>/<tablename>/<datestring>")
@cache_control("public, max-age=31536000")
@apihandler
def get_total_stats(project, tablename, datestring):
    """
    summary: return total statistics for tablename in project on datestring
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: example /total_stats/cmdb/vicenterVms/2019-08-01
    """
    _dl.setup(project, tablename, datestring)
    return _dl["total_stats"]

@app.route("/ts/<project>/<tablename>/<datestring>/<b64index>", methods=["GET"])
@cache_control("public, max-age=31536000")
@apihandler
def get_ts(project, tablename, datestring, b64index):
    """
    summary: return Timeseries for table in project on datestring
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
      - name: base64index
        in: path
        required: true
        description: base64 encoded string representation of index tuple
        schema:
          type: string
      - name: value_keyname
        in: query
        description: list of value_keynames to return, json formatted
        schema:
          type: str
    description: example /ts/cmdb/vicenterVms/2019-08-01/asdhfkasdhkajshd==
    """
    index_key = b64eval(b64index) # eval is not secure
    data = dict(request.values)
    result = {}
    _dl.setup(project, tablename, datestring)
    if data.get("value_keynames"):
        return list(_dl["tsa", index_key].to_data(data.get("value_keynames")))
    return list(_dl["tsa", index_key].to_data())

@app.route("/graph/ts/<project>/<tablename>/<datestring>/<b64index>", methods=["GET"])
@app.route("/graph/ts/<project>/<tablename>/<datestring>/<b64index>/<value_keyname>", methods=["GET"])
@cache_control("public, max-age=31536000")
@apihandler
def get_graph_ts(project, tablename, datestring, b64index, value_keyname=None):
    """
    summary: return Timeseries data specific for graphing with highcharts
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
      - name: base64index
        in: path
        required: true
        description: base64 encoded string representation of index tuple
        schema:
          type: string
      - name: value_keyname
        in: path
        required: false
        description: if given one value_keyname of this TimeSeries
        schema:
          type: string
    description: example /graph/ts/cmdb/vicenterVms/2019-08-01/asdhfkasdhkajshd==
    """
    index_key = b64eval(b64index) # eval is not secure
    _dl.setup(project, tablename, datestring)
    desc = _dl.meta
    series = {}
    value_keynames = desc["value_keynames"] # default all available
    if value_keyname and value_keyname in desc["value_keynames"]:
        value_keynames = (value_keyname, )
    for value_keyname in value_keynames:
        series[value_keyname] = {
            "name": value_keyname,
            "label": desc["label_texts"][value_keyname],
            "unit": desc["label_units"][value_keyname],
            "interval": desc["interval"],
            "data": []
        }
    for point in _dl["tsa", index_key].to_data():
        for value_keyname in value_keynames:
            series[value_keyname]["data"].append((point["ts"], point[value_keyname]))
    return list(series.values())

@app.route("/graph/tsstats/<project>/<tablename>/<datestring1>/<datestring2>/<b64index>/<stat_func_name>", methods=["GET"])
@app.route("/graph/tsstats/<project>/<tablename>/<datestring1>/<datestring2>/<b64index>/<stat_func_name>/<value_keyname>", methods=["GET"])
@cache_control("public, max-age=31536000")
@apihandler
def get_graph_tsstats(project, tablename, datestring1, datestring2, b64index, stat_func_name, value_keyname=None):
    """
    summary: return Timeseries data specific for graphing with highcharts
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring1
        in: path
        required: true
        description: datestring to start, the older one
        schema:
          type: string
      - name: datestring2
        in: path
        required: true
        description: datestring to stop, the newer one
        schema:
          type: string
      - name: base64index
        in: path
        required: true
        description: base64 encoded string representation of index tuple
        schema:
          type: string
      - name: stat_func_name
        in: path
        required: true
        description: statistical function to return for every day
        schema:
          type: string
      - name: value_keyname
        in: path
        required: false
        description: if given one value_keyname of this TimeSeries
        schema:
          type: string
    description: example /graph/tsstats/cmdb/vicenterVms/2019-08-01/2019-09-01/asdhfkasdhkajshd==/avg
    """
    index_key = b64eval(b64index) # eval is not secure
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat() # used to initialize datalogger
    _dl.setup(project, tablename, yesterday)
    desc = _dl.meta
    series = {}
    value_keynames = desc["value_keynames"] # default all available
    if value_keyname and value_keyname in desc["value_keynames"]:
        value_keynames = (value_keyname, )
    categories = [] # for x-axis, common for all value_keynames
    for value_keyname in value_keynames:
        series[value_keyname] = {
            "name": value_keyname,
            "label": desc["label_texts"][value_keyname],
            "unit": desc["label_units"][value_keyname],
            "interval": desc["interval"],
            "data": []
        }
    for datestring in _datewalker(datestring1, datestring2):
        _dl.setup(project, tablename, datestring)
        stats = _dl["tsastats", index_key].to_data()
        for value_keyname in desc["value_keynames"]:
            series[value_keyname]["data"].append((datestring, stats[value_keyname][stat_func_name]))
        categories.append(datestring)
    return {"categories" : categories, "series": list(series.values())}

@app.route("/tsstats/<project>/<tablename>/<datestring>/<b64index>", methods=["GET"])
@cache_control("public, max-age=31536000")
@apihandler
def get_tsstats(project, tablename, datestring, b64index):
    """
    summary: return TimeseriesStats for table in project on datestring
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
      - name: base64index
        in: path
        required: true
        description: base64 encoded string representation of index tuple
        schema:
          type: string
    description: example /tsstats/cmdb/vicenterVms/2019-08-01/asdhfkasdhkajshd==
    """
    index_key = b64eval(b64index) # eval is not secure
    result = {}
    _dl.setup(project, tablename, datestring)
    return _dl["tsastats", index_key].to_data()

@app.route("/project/<project>", methods=["POST"])
@apihandler
def post_project(project):
    """
    summary: create new project
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
    """
    # check string
    # must be lowercase only
    # consisting of a-z and 1-9
    # no other characters allowed
    # max. length 12
    global _dl # using global _dl
    if re.match("^[a-z0-9]{0,12}$", project) is None:
        logger.error("project %s does not match formatting criterias", project)
        raise AttributeError("format mismatch")
    if os.path.isdir(os.path.join(BASEDIR, project)):
        logger.error("project %s subdir does already exist", project)
        raise AttributeError("project subdir already exists")
    if project in _dl.get_projects():
        logger.error("project %s does already exist", project)
        raise AttributeError("project already exists")
    os.mkdir(os.path.join(BASEDIR, project))
    os.mkdir(os.path.join(BASEDIR, project, "meta"))
    os.mkdir(os.path.join(BASEDIR, project, "raw"))
    # reinit DataLogger object
    _dl = DataLogger(BASEDIR)
    logger.info("project %s created", project)
    return f"project {project} created"

@app.route("/project/<project>", methods=["DELETE"])
@apihandler
def delete_project(project):
    """
    summary: create new project
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
    """
    global _dl # using global _dl
    if project not in _dl.get_projects():
        raise AttributeError(f"project {project} does not exist")
    if _dl.get_tablenames(project): # if some tables exist
        raise AttributeError(f"project {project} has some tables, delete them first")
    os.rmdir(os.path.join(BASEDIR, project))
    os.rmdir(os.path.join(BASEDIR, project, "meta"))
    os.rmdir(os.path.join(BASEDIR, project, "raw"))
    # reinit DataLogger object
    _dl = DataLogger(BASEDIR)
    logger.info("project %s deleted", project)
    return f"project {project} deleted"

@app.route("/tablename/<project>/<tablename>", methods=["POST"])
@apihandler
def post_tablename(project, tablename):
    """
    summary: create new table in project
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
    """
    # check string
    # must be lowercase only
    # consisting of a-z and 1-9
    # no other characters allowed
    # max. length 32
    data = request.get_json()
    if re.match("^[a-zA-Z0-9]{0,32}$", tablename) is None:
        logger.error("tablename %s does not match formatting criterias", tablename)
        raise AttributeError("format mismatch")
    metafile = os.path.join(_dl.basedir, project, "meta", "%s.json" % tablename)
    if os.path.isfile(metafile):
        logger.error("table definition file for tablename %s does already exist", tablename)
        raise AttributeError("table definition file already exists")
    with open(metafile, "wt") as outfile:
        json.dump(data["meta"], outfile, indent=4)
        # reinit DataLogger object
        _dl = DataLogger(BASEDIR)
    return "table %s created" % tablename

@apihandler
def post_tablename(project, tablename):
    """
    summary: create new table in project
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
    """
    # check string
    # must be lowercase only
    # consisting of a-z and 1-9
    # no other characters allowed
    # max. length 32
    data = request.get_json()
    if re.match("^[a-zA-Z0-9]{0,32}$", tablename) is None:
        logger.error("tablename %s does not match formatting criterias", tablename)
        raise AttributeError("format mismatch")
    metafile = os.path.join(_dl.basedir, project, "meta", "%s.json" % tablename)
    if os.path.isfile(metafile):
        logger.error("table definition file for tablename %s does already exist", tablename)
        raise AttributeError("table definition file already exists")
    with open(metafile, "wt") as outfile:
        json.dump(data["meta"], outfile, indent=4)
        # reinit DataLogger object
        _dl = DataLogger(BASEDIR)
    return "table %s created" % tablename

@app.route("/raw_file/<project>/<tablename>/<datestring>", methods=["POST"])
@apihandler
def post_raw_file(project, tablename, datestring):
    """
    summary: store whole day file for tablename in project on datestring
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    description: "save received data for whole day datafile, file must not exist in prior
        file data must be placed as myfile=<bytestring> in body
        /project/tablename/datestring
        ATTENTION: requests must be sent Content-Type: multipart/form-data to work properly"
    """
    if "myfile" not in request.files:
        logger.info("No myfile part in request")
        raise AttributeError("no myfile part in request")
    _dl.setup(project, tablename, datestring)
    filename = os.path.join(_dl.raw_basedir, "%s_%s.csv.gz" % (tablename, datestring))
    if os.path.isfile(filename):
        logger.info("File already exists")
        raise AttributeError("File already exists")
    try:
        filedata = request.files["myfile"]
        with gzip.open(filename, "wb") as outfile: # has to be bytes
            logger.info("Storing data to %s", filename)
            filedata.save(outfile) # stores to filename or handle
    except Exception as exc:
        logger.exception(exc)
        os.unlink(filename)
        logger.info("Error while saving received data to %s", filename) 
        raise exc
    # reread saved data
    try:
        tsa = _dl["tsa"] # re-read received data
    except AssertionError as exc:
        logger.exception(exc)
        os.unlink(filename)
        logger.info("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
        raise Exception("Invalid data in uploaded file, see apache error log for details, uploaded file not stored")
    logger.info("File stored")
    return "File stored"

@app.route("/append/<project>/<tablename>/<clocktype>", methods=["PUT"])
@apihandler
def put_append(project, tablename, clocktype="wallclock"):
    """
    summary: store JSON data line in live datafile of today
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: clocktype
        in: path
        required: true
        description: if wallclock, check is ts in range +/- 60s, otherwise dont check
        schema:
          type: true
    description: "appending some data to actual live raw datafile
        request contains only one single line of data
        timestamp of this data must be in range of +/- 60 seconds
        otherwise set clocktype to wall or data
        TODO: quality check and return codes"
    """
    if clocktype.lower() not in ("wallclock", "dataclock"):
        raise AttributeError("clocktype must be either wallclock or dataclock")
    data = request.get_json() # get data as dict
    if "rows"  not in data: # check if key exists
        raise AttributeError("key rows not found in received data")
    if not isinstance(data["rows"], list): # of type list
        raise AttributeError("submitted data must be type list")
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).isoformat() # used to initialize datalogger
    datestring = datetime.date.today().isoformat() # today
    _dl.setup(project, tablename, yesterday) # TODO: initialization could only be done for datestring in the past
    filename = os.path.join(_dl.raw_basedir, "%s_%s.csv" % (tablename, datestring))
    firstline = False # indicate headerline or not
    if not os.path.isfile(filename):
        fh = open(filename, "wt", encoding="utf-8") # thats very important to use utf-8
        firstline = True
    else:
        fh = open(filename, "at", encoding="utf-8")
    ts = time.time() # default check timestamp, must be +/- 60s to now()
    if clocktype == "dataclock": # no check
        ts = None
    with fh as outfile:
        for row  in data["rows"]:
            valid, message, status_code = _row_is_valid(row, ts=ts) # check validity
            if valid is False:
                raise AttributeError(message)
            # actually write
            if firstline is True:
                logger.info("raw file does not exist, will create new file %s", filename)
                outfile.write("\t".join(_dl.headers) + "\n") # TODO: use delimiter defined in meta
                firstline = False
            outfile.write("\t".join([str(row[key]) for key in _dl.headers]) + "\n")
    return "%d rows appended" % len(data["rows"])

@app.route("/appendmany/<project>/<tablename>", methods=["PUT"])
@apihandler
def put_appendmany(project, tablename):
    """
    summary: store a bunch of data lines in live datafile of today
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
    description: "appening many data rows to some project/tablename -s actual live data
        for historical data use post_raw_file
        request contains many lines of data
        every lines timestamp must be in range +/- 60s
        TODO: quality check and return codes, and improvements"
    """
    # DEPRECATED: delete this endpoint in future versions
    return put_append(project, tablename, clocktype="wallclock")

@app.route("/caches/<project>/<tablename>/<datestring>", methods=["DELETE"])
@apihandler
def delete_caches(project, tablename, datestring):
    """
    summary: delete analyzed data, raw input data will be left untouched
    parameters:
      - name: project
        in: path
        required: true
        description: name of project
        schema:
          type: string
      - name: tablename
        in: path
        required: true
        description: name of table
        schema:
          type: string
      - name: datestring
        in: path
        required: true
        description: datestring like 2019-12-31
        schema:
          type: string
    """
    _dl.setup(project, tablename, datestring)
    _dl.delete_caches()


############### private functions ##################################

def _get_table_index(project, tablename, datestring):
    """
    yield all stored data form project/tablename/datestring
    :param project: <str>
    :param tablename: <str>
    :param datestring: <str> like 2019-012-31
    :return <dict>: generator
    """
    _dl.setup(project, tablename, datestring)
    dl_caches = dict(_dl["caches"])
    for str_key in dl_caches["ts"]["keys"]:
        filename = dl_caches["ts"]["keys"][str_key]
        b64_key = filename.split(".")[0].split("_")[1]
        index_dict = {
            "_project": project,
            "_tablename": tablename,
            "_datestring": datestring,
            "_key": dict(zip(_dl.meta["index_keynames"], eval(str_key))),
            "_str_key": str_key,
            "_b64_key": b64_key,
            "_links": {
                "ts": "/".join(("ts", project, tablename, datestring, b64_key)),
                "tsstat": "/".join(("tsstat", project, tablename, datestring, b64_key))
            }
        }
        # add index in dict form
        # index_dict.update(dict(zip(_dl.meta["index_keynames"], eval(str_key))))
        yield index_dict

@fs_cache
def _get_index(datestring):
    """
    return list of timeseries available of all projects/tablenames
    """
    ret_data = []
    for project in list(_dl.get_projects()):
        for tablename in list(_dl.get_tablenames(project)):
            _dl.setup(project, tablename, datestring)
            index_keynames = _dl.meta["index_keynames"]
            for index in _dl["caches"]["ts"]["keys"]:
                # key like : ts_KHUndnNhbmFwcDYnLCB1JzEwMicsIHUnMCcsIHUnMScsIHUnMzUnLCB1J1ByaW1hcnkgTGF5b3V0JywgdScxMDAyNicp.csv.gz
                b64 = _dl["caches"]["ts"]["keys"][index].split("_")[1].split(".")[0]
                index_dict = {
                    "_project": project,
                    "_tablename": tablename,
                    "_datestring": datestring,
                    "_str_index_key": index,
                    "_b64_index_key": b64,
                    "_links": {
                        "ts" : "/".join(("ts", project, tablename, datestring, b64)),
                        "tsstat" : "/".join(("tsstat", project, tablename, datestring, b64))
                    }
                }
                index_dict.update(dict(zip(index_keynames, eval(index))))
                ret_data.append(index_dict)
    return ret_data

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

def _row_is_valid(row, ts=None):
    """
    return True if row is valid, otherwise False, cehck timestamp if ts was given
    :params row <dict>: dictionary of single row
    :params ts None or float: if given check if timestamp is in between +/- 60s to now
    :returns <tuple>(<bool:valid or not>, <str:message>, <int:status:code>):
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
    if ts: # only if given
        # check timestamp
        min_ts = ts - 60
        max_ts = ts + 60
        if not min_ts < float(row[_dl.ts_keyname]) < max_ts:
            logger.info("timestamp in received data is out of range +/- 60s")
            return False, "timestamp in received data is out of range +/- 60s", 409
    return True, "row is valid", 200

def _datestring_to_date(datestring):
    """
    function to convert datestring to datetime object
    :param <str> datestring:
    :return <datetime.date>:
    """
    year, month, day = datestring.split("-")
    return datetime.date(int(year), int(month), int(day))

def _datewalker(datestring_start, datestring_stop):
    """
    function to walk from beginning datestring to end datestring,
    in steps of one day
    :param <str> datestring_start: the older datestring
    :param <str> datestring_stop: the newer datestring
    :returns <generator>: datestring from (including) datestring_start to datestring_stop
    """
    start_date = _datestring_to_date(datestring_start)
    stop_date = _datestring_to_date(datestring_stop)
    while start_date <= stop_date:
        yield start_date.isoformat()
        start_date = start_date + datetime.timedelta(days=1)

#################### global initialization ####################

logger = logging.getLogger("DataLoggerWebApp")
logger.setLevel(logging.INFO)
application = app # WSGI Module will call application.run()
app.config["JSON_AS_ASCII"] = False # that little thing is crucial, json will return UTF-8 encoded
if __name__ == "__main__":
    print("started as standalone flask app")
    BASEDIR = "test/testdata"
    _dl = DataLogger(BASEDIR)
    with app.test_client() as client:
        print(client.get("/doc").get_json()) # swagger missing
        print(client.get("/hello").get_json())
        #def get_ts_for_datestring(datestring):
        print(client.get("/ts_for_datestring/2020-01-01").get_json())
        #def get_yesterday_datestring():
        print(client.get("/yesterday_datestring").get_json())
        #def get_last_business_day_datestring():
        print(client.get("/last_business_day_datestring").get_json())
        #def get_datestrings():
        print(client.get("/datestrings").get_json())
        #def get_stat_func_names():
        print(client.get("/stat_func_names").get_json())
        #def get_projects():
        print(client.get("/projects").get_json())
        #def get_tablenames(project):
        print(client.get("/tablenames/sanportperf").get_json())
        print(client.get("/tablenames/mysql").get_json())
        #def get_desc(project, tablename):
        print(client.get("/desc/sanportperf/fcIfC3AccountingTable").get_json())
        print(client.get("/desc/mysql/performance").get_json())
        #def get_exists(project, tablename, datestring):
        print(client.get("/exists/sanportperf/fcIfC3AccountingTable/2020-01-01").get_json())
        print(client.get("/exists/sanportperf/fcIfC3AccountingTable/2018-04-01").get_json())
        print(client.get("/exists/mysql/performance/2018-04-01").get_json())
        #def get_index(project, tablename, datestring):
        print(client.get("/index/mysql/performance/2018-04-01").get_json())
        #def get_search(datestring, pattern):
        print(client.get("/search/2018-04-01/mysql").get_json())
        #def get_ts(project, tablename, datestring, b64index):
        print(client.get("/ts/mysql/performance/2018-04-01/KHUnbmFnaW9zLnRpbGFrLmNjJywp").get_json())
        #def get_tsstats(project, tablename, datestring, b64index):
        print(client.get("/tsstats/mysql/performance/2018-04-01/KHUnbmFnaW9zLnRpbGFrLmNjJywp").get_json())
        #def get_quantile(project, tablename, datestring):
        print(client.get("/quantile/mysql/performance/2018-04-01").get_json()) # TODO: does not return correct data
        #def get_stats_by_value_keyname(project, tablename, datestring, value_keyname):
        # TODO: is this useful
        #def get_stats_by_func(project, tablename, datestring, stat_func_name):
        # TODO: is this useful
        #def get_total_stats(project, tablename, datestring):
        print(client.get("/total_stats/mysql/performance/2018-04-01").get_json())
        #def get_graph_ts(project, tablename, datestring, b64index, value_keyname=None):
        #def get_graph_tsstats(project, tablename, datestring1, datestring2, b64index, stat_func_name, value_keyname=None):
        #def post_project(project):
        print(client.post("/project/testproject").get_json())
        # def delete_project(project):
        print(client.delete("/project/testproject").get_json())
        #def post_tablename(project, tablename):
        #def post_raw_file(project, tablename, datestring):
        #def put_append(project, tablename, clocktype="wallclock"):
        #def put_appendmany(project, tablename):
        #def delete_caches(project, tablename, datestring):
    #app.run()
else:
    # started as wsgi module
    # hold global config
    #with open("/var/www/config/DataLoggerWebApp.yaml", "rt") as infile:
    #    app.config["app_config"] = yaml.load(infile)
    #    logger.debug(app.config["app_config"])
    #CONFIG = app.config["app_config"] # shortcut
    #BASEDIR = CONFIG["basedir"]
    _dl = DataLogger(BASEDIR)
