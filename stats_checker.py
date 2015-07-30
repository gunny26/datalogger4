#!/usr/bin/python
import operator
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from tilak_datalogger import DataLogger as DataLogger
from commons import *

def get_mse(series1, series2):
    """
    the series as is
    """
    assert len(series1) == len(series2)
    mse = 0.0
    for index, data1 in enumerate(series1):
        diff = (data1 - series2[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_sorted(series1, series2):
    """
    sorted but not normalized values
    """
    assert len(series1) == len(series2)
    mse = 0.0
    series2_s = sorted(series2)
    for index, data1 in enumerate(sorted(series1)):
        diff = (data1 - series2_s[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_norm(series1, series2):
    """
    normalized values
    """
    assert len(series1) == len(series2)
    mse = 0.0
    max_v = max(max(series1), max(series2))
    s1 = tuple((value/max_v for value in series1))
    s2 = tuple((value/max_v for value in series2))
    for index, data1 in enumerate(s1):
        diff = (data1 - s2[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_sorted_norm(series1, series2):
    """
    sorted and normalized
    """
    assert len(series1) == len(series2)
    mse = 0.0
    max_v = max(series1)
    s1 = tuple((value/max_v for value in sorted(series1)))
    s2 = tuple((value/max_v for value in sorted(series2)))
    for index, data1 in enumerate(s1):
        diff = (data1 - s2[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_max_outs(series, max_value):
    """
    get the percentage the maximum is reached or beyond
    """
    print float(100 * sum((value > max_value for value in series)))/len(series)

def get_correlating(datalogger, datestring, key, group_func_name, value_key):
    ts = datalogger.read_ts(get_last_business_day_datestring(), key, group_func_name)
    print "Searching for correlation to %s, grouped by %s, value_key %s)" % (str(key), group_func_name, value_key)
    series = ts.get_serie(value_key)
    matrix = []
    for func, args in datalogger.get_ts_caches(datestring):
        other = func(*args).get_serie(value_key)
        try:
            matrix.append((
                #get_mse(series, other),
                #get_mse_sorted(series, other),
                #get_mse_norm(series, other),
                get_mse_sorted_norm(series, other),
                args[1:]
                ))
        except AssertionError:
            print "skipping %s" % str(args)
    return matrix

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    #for caches in datalogger.get_tsa_caches(datestring):
    #    print caches[1]
    #    #obj = caches[0](*caches[1])
    #for caches in datalogger.get_tsastat_caches(datestring):
    #    print caches[1]
    #    #obj = caches[0](*caches[1])
    #    #print obj.get_stats("rate")
    others = []
    for caches in datalogger.get_ts_caches(datestring):
        print caches[1]
        others.append(caches)
    #    #obj = caches[0](*caches[1])
    #ts = datalogger.read_ts(get_last_business_day_datestring(), ("srvapamed1.tilak.cc","0"), None)
    #print ts.headers
    #print "Searching for correlation to (srvapamed1.tilak.cc, 0), None)"
    #series = ts.get_serie("cpu.used.summation")
    ts = datalogger.read_ts(get_last_business_day_datestring(), ((u"srvapamed1.tilak.cc",u"0")), None)
    get_max_outs(ts.get_serie("cpu.used.summation"), 20000 * 0.95)
    return
    matrix = get_correlating(datalogger, datestring, (("srvapexfs8.tilak.cc","0")), None, "cpu.used.summation")
    for row in sorted(matrix, key=operator.itemgetter(0)):
        print "%0.3f\t%s" % row
    return
    return
    tsa = datalogger.read_tsa_full(datestring, force=False, timedelta=0)
    # sanitize data
    tsa.sanitize()
    tsa.add_per_s_col('bin', 'bin_s')
    tsa.add_per_s_col('bout', 'bout_s')
    tsa.remove_col('bin')
    tsa.remove_col('bout')
    tsa_grouped = tsa.slice(("bin_s", "bout_s"))
    standard_wiki_report(datalogger, datestring, tsa, tsa_grouped)

def main():
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
