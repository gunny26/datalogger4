#!/usr/bin/python
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from tilak_datalogger import DataLogger as DataLogger
from commons import *

def main():
    basedir = "/var/rrd/"
    data = {
        "haproxy" : [
            "backend",
            "frontend",
            "server",
            ],
        "ipstor" : [
            "vrsClientTable",
            ],
        "sanportperf" : [
            "fcIfC3AccountingTable",
            ],
        "vicenter" : [
            "hostSystemCpuStats",
            "hostSystemDiskStats",
            #"hostSystemMemoryStats",
            "hostSystemNetworkStats",
            "virtualMachineCpuStats",
            "virtualMachineDatastoreStats",
            "virtualMachineDiskStats",
            "virtualMachineMemoryStats",
            "virtualMachineNetworkStats",
        ],
    }
    group_funcs = {
        "sum" : lambda a: sum(a),
        "min" : lambda a: min(a),
        "max" : lambda a: max(a),
        "avg" : lambda a: sum(a) / len(a),
        "len" : lambda a: len(a),
    }
    datestring = get_last_business_day_datestring()
    for project, tablenames in data.items():
        for tablename in tablenames:
            logging.info("reading raw data and creating caches for %s %s %s", project, tablename, datestring)
            datalogger = DataLogger(basedir, project, tablename)
            tsa = datalogger.read_day(datestring)
            tsa.sanitize()
            logging.info("creating grouped caches")
            # group by every index_keyname, use sum group function
            for subkey in tsa.get_index_keys():
                for name, func in group_funcs.items():
                    logging.info("grouping %s by function %s", subkey, name)
                    tsa_grouped = tsa.get_group_by_tsa((subkey,), group_func=lambda a: sum(a))
                    #datalogger.save_cachefile_single(datestring, tsa_grouped)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
