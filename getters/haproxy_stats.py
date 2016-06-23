#!/usr/bin/python
"""
program to get haproxy statistics data and to put it into shape for datalogger

you have to customize config file to put necessary data into it
"""
import sys
import urllib2
import time
import datetime
import os
import argparse
import json
from collections import OrderedDict
import logging
logging.basicConfig(level=logging.ERROR)

def get_haproxy_csv(servername, username, password):
    # for basic authentication
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    # add authentication information
    password_mgr.add_password(None, servername, username, password)
    # create handler
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    # create opener
    opener = urllib2.build_opener(handler)
    # open url
    res = opener.open("http://%s/haproxy?stats;csv" % servername)
    csv = res.read().split('\n')
    return(csv)

def get_data(haproxies, index_keynames):
    ret_data = OrderedDict()
    timestamp = time.time()
    for clustername, server_dict in haproxies.items():
        for servername, credentials in server_dict.items():
            csv = get_haproxy_csv(servername, credentials["username"], credentials["password"])
            headers = csv[0].split(",")
            headers[0] = headers[0][2:] # cut off trailing #
            #print headers
            for row in csv[1:]:
                if len(row) > 0:
                    #print row
                    values = row.split(",")
                    dict_values = OrderedDict()
                    dict_values[u"ts"] = str(timestamp)
                    dict_values[u"servername"] = servername
                    dict_values[u"clustername"] = clustername
                    for key, value in dict(zip(headers, values)).items():
                        if key == "":
                            continue
                        dict_values[key] = value
                    #print dict_values
                    index_value = tuple((dict_values[keyname] for keyname in index_keynames))
                    try:
                        assert index_value not in ret_data.keys() # index must be unique
                    except AssertionError as exc:
                        logging.error("AssertionError at %s/%s index_value=%s, ret_data=%s", clustername, servername, index_value, ret_data)
                        raise exc
                    ret_data[index_value] = dict_values
    return ret_data

def save_data(basedir_raw, tablename, data):
    """
    save data in data to raw file with isoformat extention in basedir_raw
    """
    # output full dataset
    filename = os.path.join(basedir_raw, "%s_%s.csv" % (tablename, datetime.date.today().isoformat()))
    outfile = None
    headers = True
    if os.path.exists(filename):
        # open in append mode if file exists
        outfile = open(filename, "ab")
        # and print headers
        headers = False
    else:
        outfile = open(filename, "wb")
    lines = 0
    for keyid, values in data.items():
        if headers is True:
            outfile.write("%s\n" % "\t".join(values.keys()))
            headers = False
        try:
            outfile.write("%s\n" % "\t".join(values.values()))
        except KeyError as exc:
            logging.error("DataformatError skipping %s : %s", keyid, data[keyid])
        except TypeError as exc:
            logging.exception(exc)
            logging.error(data[keyid])
        lines += 1
    logging.info("got %d datasets", lines)
    logging.info("got %d unique keys", len(data.keys()))
    outfile.close()


def main():
    project = "haproxy"
    # tablename will be auto generated
    # initialize project
    basedir = os.path.join("/var/rrd", project)
    raw_basedir = os.path.join(basedir, "raw")
    meta_basedir = os.path.join(basedir, "meta")
    if not os.path.exists(basedir):
        os.mkdir(basedir)
        os.mkdir(raw_basedir)
        os.mkdir(meta_basedir)
    # get DATA
    configfilename = "%s.json" % sys.argv[0].split(".")[0]
    logging.info("configfilename: %s", configfilename)
    if not os.path.isfile(configfilename):
        logging.error("config file %s not found", configfilename)
        sys.exit(1)
    haproxies = json.load(open(configfilename))
    index_keynames = ("clustername", "servername", "pxname", "svname")
    data = get_data(haproxies, index_keynames)
    # get data in shape
    tablename_dict = {
        "0" : {
            "ts_keyname" : u"ts",
            "delimiter" : u"\t",
            "tablename" : u"frontend",
            "values" : {},
            "index_keynames" : index_keynames,
            "headers" : None,
            "value_keynames" : [],
            "blacklist" : [],
            "deleted" : [],
        },
        "1" : {
            "ts_keyname" : u"ts",
            "delimiter" : u"\t",
            "tablename" : u"backend",
            "values" : {},
            "index_keynames" : index_keynames,
            "headers" : None,
            "value_keynames" : [],
            "blacklist" : [],
            "deleted" : [],
        },
        "2" : {
            "ts_keyname" : u"ts",
            "delimiter" : u"\t",
            "tablename" : u"server",
            "values" : {},
            "index_keynames" : index_keynames,
            "headers" : None,
            "value_keynames" : [],
            "blacklist" : [],
            "deleted" : [],
        },
        "3" : {
            "ts_keyname" : u"ts",
            "delimiter" : u"\t",
            "tablename" : u"listener",
            "values" : {},
            "index_keynames" : index_keynames,
            "headers" : None,
            "value_keynames" : [],
            "blacklist" : [],
            "deleted" : [],
        },
    }
    # split into different tables, get rid of empty fields for special
    # types, generate blacklist keys for every type
    # only numeric fields are values, all other keys appear on blacklist
    for key, values in data.items():
        tablename_dict[values["type"]]["headers"] = values.keys()
        tablename_dict[values["type"]]["values"][key] = values
        for v_key, v_value in values.items():
            if v_value == "":
                if not v_key in tablename_dict[values["type"]]["deleted"]:
                    tablename_dict[values["type"]]["deleted"].append(v_key)
                del tablename_dict[values["type"]]["values"][key][v_key]
                tablename_dict[values["type"]]["headers"].remove(v_key)
            try:
                float(v_value)
                if not v_key in tablename_dict[values["type"]]["value_keynames"]:
                    tablename_dict[values["type"]]["value_keynames"].append(v_key)
            except ValueError:
                if not v_key in tablename_dict[values["type"]]["blacklist"] and not v_key in tablename_dict[values["type"]]["index_keynames"]:
                    tablename_dict[values["type"]]["blacklist"].append(v_key)
    # output part
    for tablekey, metainfo in tablename_dict.items():
        #print metainfo["tablename"]
        #print metainfo["headers"]
        logging.debug("Type: ", metainfo["tablename"])
        meta = {
            "ts_keyname" : metainfo["ts_keyname"],
            "blacklist" : metainfo["blacklist"],
            "headers" : metainfo["headers"],
            "delimiter" : metainfo["delimiter"],
            "value_keynames" : metainfo["value_keynames"],
            "index_keynames" : metainfo["index_keynames"],
        }
        logging.debug(meta)
        for key, series in metainfo["values"].items():
            logging.debug(series.values())
            # is some servers are down, this assertion is a mismatch
            #try:
            #    assert  metainfo["headers"] == series.keys()
            #except AssertionError as exc:
            #    logging.exception(exc)
            #    logging.error("%s : metainfo[headers] : %s and series.keys() : %s, are not the same", key, metainfo["headers"], series.keys())
        # dump data
        save_data(raw_basedir, metainfo["tablename"], metainfo["values"])
        # dump meta information
        metafile = os.path.join(meta_basedir, "%s.json" % metainfo["tablename"])
        if not os.path.isfile(metafile):
            logging.info("auto generating datalogger table configuration to %s", metafile)
            json.dump(meta, open(metafile, "wb"))

if __name__ == "__main__":
    main()
