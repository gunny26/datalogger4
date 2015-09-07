#!/usr/bin/python
import sys
import logging

project = sys.argv[1]
tablename = sys.argv[2]
datestring = sys.argv[3]

filename = "report_%s_%s" % (project, tablename)
print filename
try:
    from filename import report as func
    print func.__name__
except ImportError as exc:
    logging.exception(exc)

