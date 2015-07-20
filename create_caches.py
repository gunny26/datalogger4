#!/usr/bin/python
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from tilak_datalogger import DataLogger as DataLogger
from tilak_datalogger import TimeseriesArrayStats as TimeseriesArrayStats
from commons import *

def main():
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
#            "hostSystemCpuStats",
            "hostSystemDiskStats",
#            #"hostSystemMemoryStats",
#            "hostSystemNetworkStats",
            "virtualMachineCpuStats",
#            "virtualMachineDatastoreStats",
#            "virtualMachineDiskStats",
#            "virtualMachineMemoryStats",
#            "virtualMachineNetworkStats",
        ],
    }
    group_funcs = {
        "sum" : lambda a: sum(a),
        "min" : lambda a: min(a),
        "max" : lambda a: max(a),
        "avg" : lambda a: sum(a) / len(a),
        "len" : lambda a: len(a),
    }
    force = False
    datestring = get_last_business_day_datestring()
    for project, tablenames in data.items():
        for tablename in tablenames:
            logging.info("reading raw data and creating caches for %s %s %s", project, tablename, datestring)
            datalogger = DataLogger(BASEDIR, project, tablename)
            tsa = datalogger.read_tsa_full(datestring)
            tsa.sanitize()
            tsastat = TimeseriesArrayStats(tsa)
            logging.info("creating grouped caches")
            # group by every index_keyname, use sum group function
            for subkey in tsa.get_index_keys():
                for name, func in group_funcs.items():
                    logging.info("grouping %s by function %s", subkey, name)
                    tsa_grouped = tsa.get_grouped_tsa((subkey,), group_func_name=name)
                    index_keys = tsa_grouped.get_index_keys()
                    assert type(index_keys) == tuple
                    logging.error("writing caches for key %s", index_keys)
                    datalogger.save_tsa_full(datestring, tsa_grouped, group_func_name=name, force=force)
                    datalogger.save_tsa_split(datestring, tsa_grouped, group_func_name=name, force=force)
                    tsastat = TimeseriesArrayStats(tsa_grouped)
                    datalogger.save_tsastat_full(datestring, tsastat, group_func_name=name, force=force)
                    # check stored data
                    logging.error("reading caches for key %s", index_keys)
                    tsastat_check = datalogger.read_tsastat_full(datestring, index_keys, group_func_name=name)
                    assert all(key in tsastat.keys() for key in  tsastat_check.keys())
                    tsa_check = datalogger.read_tsa_full(datestring, index_keys, group_func_name=name)
                    try:
                        assert all(key in tsa_grouped.keys() for key in tsa_check.keys())
                    except AssertionError:
                        logging.error("Objects are not equal")
                        print tsa_grouped.keys()
                        print tsa_check.keys()
                    for key in tsa_grouped.keys():
                        ts_check = datalogger.read_ts(datestring, key, name)
                        assert ts_check == tsa_grouped[key]

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
