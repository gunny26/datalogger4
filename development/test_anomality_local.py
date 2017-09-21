#!/usr/bin/pypy
import cProfile
import time
import json
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLogger as DataLogger
from datalogger import DataLoggerWeb as DataLoggerWeb
from datalogger import CorrelationMatrixTime as CorrelationMatrixTime
from commons import *

def get_mse(series1, series2):
    """
    mean squared error - error is always positive
    the greater the resulting value, the more different are those two series

    the series are used as given, neither sorted or normalized
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

    the series will be sorted
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

    the series will be sorted and normalized to maximum value in any series
    maximum value = 1.0
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

    series will be sorted and normalized
    """
    assert len(series1) == len(series2)
    mse = 0.0
    max_v = max(series1)
    if max_v == 0.0:
        # difference is equa series2
        return sum((value * value for value in series2))/len(series1)
    s1 = tuple((value/max_v for value in sorted(series1)))
    s2 = tuple((value/max_v for value in sorted(series2)))
    for index, data1 in enumerate(s1):
        diff = (data1 - s2[index])
        mse += diff * diff
    mse /= len(series1)
    return mse

def get_mse_sorted_norm_missing(series1, series2):
    """
    sorted and normalized

    series will be sorted and normalized
    this function will not show any result where the two series are not of same length
    """
    mse = 0.0
    max_v = max(series1)
    if max_v == 0.0:
        # difference is equa series2
        return sum((value * value for value in series2))/len(series1)
    s1 = tuple((value/max_v for value in sorted(series1, reverse=True)))
    s2 = tuple((value/max_v for value in sorted(series2, reverse=True)))
    assert abs(len(s1) - len(s2)) / max(len(s1), len(s2)) < 0.1 # not more than 10% length difference
    for index, data1 in enumerate(s1):
        try:
            diff = (data1 - s2[index])
            mse += diff * diff
        except IndexError:
            break
    mse /= len(series1)
    return mse


class CorrelationMatrixArray(object):
    """
    compare every index_key with every other index_key on the same value_key
    """

    def __init__(self, tsa):
        self.__data = {}
        for value_key in tsa.value_keys:
            print "calculating value_key %s" % value_key
            self.__data[value_key] = CorrelationMatrix(tsa, value_key)

    def __eq__(self, other):
        try:
            assert self.__data.keys() == other.keys()
            for key in self.__data.keys():
                assert self.__data[key] == other[key]
            return True
        except AssertionError as exc:
            logging.exception(exc)
            print self.__data.keys(), other.keys()
        return False

    def __getitem__(self, key):
        return self.__data[key]

    def keys(self):
        return self.__data.keys()

    def dump(self, filehandle):
        data = dict(((key, self.__data[key].dumps()) for key in self.__data.keys()))
        json.dump(data, filehandle)

    @staticmethod
    def load(filehandle):
        cma = CorrelationMatrixArray.__new__(CorrelationMatrixArray)
        data = json.load(filehandle)
        cma.__data = dict(((key, CorrelationMatrix.loads(data)) for key, data in data.items()))
        return cma


def report_group(project, tablename, datestring1, datestring2, value_key):
    # get data, from datalogger, or dataloggerhelper
    datalogger = DataLogger(BASEDIR, project, tablename)
    #dataloggerweb = DataLoggerWeb(DATALOGGER_URL)
    print "loading data"
    starttime = time.time()
    logging.info("loading TSA 1")
    tsa1 = datalogger.load_tsa(datestring1)
    #tsa1 = dataloggerweb.get_tsa(project, tablename, datestring1)
    logging.info("grouping TSA 1")
    tsa1 = datalogger.group_by(datestring1, tsa1, ("hostname",), lambda a,b : (a+b)/2)
    print tsa1.keys()
    logging.info("loading TSA 2")
    tsa2 = datalogger.load_tsa(datestring2)
    #tsa2 = dataloggerweb.get_tsa(project, tablename, datestring2)
    logging.info("grouping TSA 2")
    tsa2 = datalogger.group_by(datestring2, tsa2, ("hostname",), lambda a,b : (a+b)/2)
    print tsa2.keys()
    print "Duration load %f" % (time.time() - starttime)
    starttime = time.time()
    logging.info("building CorrelationMatrix")
    cm = CorrelationMatrixTime(tsa1, tsa2, value_key)
    print "TOP most differing keys between %s and %s" % (datestring1, datestring2)
    for key, coefficient in sorted(cm.items(), key=lambda items: items[1], reverse=True)[:20]:
        print key, coefficient

def report(project, tablename, datestring1, datestring2, value_key):
    # get data, from datalogger, or dataloggerhelper
    datalogger = DataLogger(BASEDIR, project, tablename)
    starttime = time.time()
    logging.info("loading TSA 1")
    tsa1 = datalogger.load_tsa(datestring1)
    logging.info("loading TSA 2")
    tsa2 = datalogger.load_tsa(datestring2)
    print "Duration load %f" % (time.time() - starttime)
    starttime = time.time()
    logging.info("building CorrelationMatrix")
    cm = CorrelationMatrixTime(tsa1, tsa2, value_key)
    uid = hash((datestring1, datestring2, value_key))
    print "unique hash value of this matrix %s" % uid
    print "TOP most differing keys between %s and %s" % (datestring1, datestring2)
    for key, coefficient in sorted(cm.items(), key=lambda items: items[1], reverse=True)[:20]:
        print key, coefficient

def main():
    project = "vicenter"
    tablename = "virtualMachineMemoryStats"
    dataloggerweb = DataLoggerWeb(DATALOGGER_URL)
    datestring = dataloggerweb.get_last_business_day_datestring()
    year, month, day = datestring.split("-")
    date1 = datetime.date(int(year), int(month), int(day))
    print date1
    date2 = date1 - datetime.timedelta(days=7)
    print date2.isoformat()
    #report_group("vicenter", "virtualMachineCpuStats", datestring, date2.isoformat(), "cpu.used.summation")
    #report_group("sanportperf", "fcIfC3AccountingTable", datestring, date2.isoformat(), "fcIfC3InOctets")
    #report(project, tablename, datestring, date2.isoformat(), "mem.active.average")
    #report("vicenter", "virtualMachineDatastoreStats", datestring, date2.isoformat(), "datastore.totalReadLatency.average")
    #report("vicenter", "virtualMachineDatastoreStats", datestring, date2.isoformat(), "datastore.write.average")
    #report("vicenter", "virtualMachineNetworkStats", datestring, date2.isoformat(), "net.usage.average")
    report("haproxy", "http_host", "2016-03-29", "2016-03-15", "hits")
    #report("sanportperf", "fcIfC3AccountingTable", datestring, date2.isoformat(), "fcIfC3InOctets")

if __name__ == "__main__":
    #main()
    cProfile.run("main()")
