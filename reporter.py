#!/usr/bin/python
import sys
import logging
import importlib
from tilak_datalogger import DataLogger as DataLogger
from commons import *

projects = None
tablenames = None

project = sys.argv[1]
if project == "ALL":
    projects = DataLogger.get_projects(BASEDIR)
else:
    assert project in DataLogger.get_projects(BASEDIR)
    projects = (project, )

tablename = sys.argv[2]
if tablename == "ALL":
    tablenames = DataLogger.get_tablenames(BASEDIR, project)
else:
    assert tablename in DataLogger.get_tablenames(BASEDIR, project)
    tablenames = (tablename, )

datestring = sys.argv[3]
for project in projects:
    for tablename in tablenames:
        filename = "report_%s_%s" % (project, tablename)
        #print filename
        try:
            datalogger = DataLogger(BASEDIR, project, tablename)
            datestring = get_last_business_day_datestring()
            func = importlib.import_module(filename)
            print "calling %s" % func.__name__
            try:
                func.report(datalogger, datestring)
            except StandardError as exc:
                logging.exception(exc)
        except ImportError as exc:
            logging.exception(exc)

