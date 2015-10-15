#!/usr/bin/pypy
import cProfile
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLogger as DataLogger
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

def get_correlating(tsa, value_key):
    """
    search for corelating series in all other series available
    """
    print "Searching for correlation in value_key %s)" % value_key
    matrix = {}
    for key in tsa.keys()[:20]:
        series = tsa[key][value_key]
        matrix[key] = {}
        for otherkey in tsa.keys()[:20]:
            other = tsa[otherkey][value_key]
            if len(series) == len(other):
                matrix[key][otherkey] = get_mse_sorted_norm(series, other)
            else:
                print "skipping, dataseries are not of same length"
    return matrix

def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    tsa3 = datalogger.group_by(datestring, tsa, ("hostname", ), lambda a,b: (a + b) / 2)
    # tsa_test = tsa.slice(("cpu.used.summation", ))
    matrix = get_correlating(tsa3, "cpu.used.summation")
    for key, rowdict in matrix.items():
        print str(key) + "\t" + "\t".join(("%0.2f" % value for value in rowdict.values()))

def main():
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    #main()
    cProfile.run("main()")
