#!/usr/bin/pypy
import cProfile
import time
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
import datalogger
from datalogger import DataLogger as DataLogger
from datalogger import QuantillesArray as QuantillesArray
from commons import *

def main():
    project = "vdi"
    tablename = "virtualMachineMemoryStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    qa = datalogger.load_quantilles("2016-02-22")
    ret_data = []
    # build header
    ret_data.append(list(datalogger.index_keynames) + ["Q0", "Q1", "Q2", "Q3", "Q4"])
    # data part
    for k, v  in qa["mem.overhead.average"].quantilles.items():
        ret_data.append(list(k) + v.values())
    print json.dumps(ret_data)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
