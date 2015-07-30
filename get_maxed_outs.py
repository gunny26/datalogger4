#!/usr/bin/python
import operator
import numpy
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

def get_max_outs(datalogger, datestring, value_key, max_value):
    """
    get the percentage the maximum is reached or beyond
    like quantile(0.95)
    """
    matrix = []
    for func, args in datalogger.get_ts_caches(datestring):
        series = func(*args).get_serie(value_key)
        matrix.append((
            float(100 * sum((value > max_value for value in series)))/len(series),
            args[1:],
            ))
    return matrix

def get_cpu_usage_std(datalogger, datestring):
    """
    calculates std between cpu cores to find unweight cpu usage for servers
    """
    value_key = "cpu.used.summation"
    matrix = []
    cpu_dict = {}
    # collect series by vm_name
    for func, args in datalogger.get_ts_caches(datestring):
        vm_name = args[1:][0][0]
        series = func(*args).get_serie(value_key)
        if vm_name not in cpu_dict:
            cpu_dict[vm_name] = [series, ]
        else:
            cpu_dict[vm_name].append(series)
    # calculate std between each measurepoint
    for vm_name, series_list in cpu_dict.items():
        data = []
        for index in range(len(series_list[0])):
            value = numpy.std(tuple((serie[index] for serie in series_list)))
            data.append(value)
        # sum up all stds
        matrix.append((
            sum(data)/len(data),
            vm_name,
            ))
    return matrix

def get_correlating(datalogger, datestring, key, group_func_name, value_key):
    """
    search for corelating series in all other series available
    """
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
    others = []
    for caches in datalogger.get_ts_caches(datestring):
        print caches[1]
        others.append(caches)
    #matrix = get_cpu_usage_std(datalogger, datestring)
    matrix = get_max_outs(datalogger, datestring, "cpu.used.summation", 20000 * 0.95)
    for row in sorted(matrix, key=operator.itemgetter(0), reverse=True):
        print "%0.3f\t%s" % row
    return

def main():
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
