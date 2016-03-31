#!/usr/bin/pypy
import cProfile
import time
import json
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLogger as DataLogger
from datalogger import DataLoggerWeb as DataLoggerWeb
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


class CorrelationMatrixTime(object):

    def __init__(self, tsa1, tsa2, value_key):
        self.__data = self.__get_correlation_matrix(tsa1, tsa2, value_key)

    @property
    def data(self):
        return self.__data

    def __eq__(self, other):
        try:
            assert self.__data == other.data
            return True
        except AssertionError as exc:
            logging.exception(exc)
            print self.__data.keys(), other.data.keys()
        return False

    def __getitem__(self, key):
        print key
        return self.__data[key]

    def keys(self):
        return self.__data.keys()

    def values(self):
        return self.__data.values()

    def items(self):
        return self.__data.items()

    @staticmethod
    def __get_correlation_matrix(tsa1, tsa2, value_key):
        """
        search for corelating series in all other series available
        """
        print "Searching for correlation in value_key %s)" % value_key
        matrix = {}
        keylist = tsa1.keys()
        for key in keylist:
            other = None
            try:
                other = tsa2[key][value_key]
            except KeyError:
                print "key %s is not in older tsa, skipping" % str(key)
                continue
            series = tsa1[key][value_key]
            matrix[key] = get_mse_sorted_norm_missing(series, other)
            #print key, matrix[key]
        return matrix

    def dumps(self):
        return json.dumps(str(self.__data))

    @staticmethod
    def loads(data):
        cm = CorrelationMatrix.__new__(CorrelationMatrix)
        cm.__data = eval(json.loads(data))
        return cm

def report_group(project, tablename, datestring1, datestring2, value_key):
    # get data, from datalogger, or dataloggerhelper
    datalogger = DataLogger(BASEDIR, project, tablename)
    dataloggerweb = DataLoggerWeb(DATALOGGER_URL)
    print "loading data"
    starttime = time.time()
    #tsa1 = datalogger.load_tsa(datestring1)
    tsa1 = dataloggerweb.get_tsa(project, tablename, datestring1)
    tsa1 = datalogger.group_by(datestring1, tsa1, ("hostname",), lambda a,b : (a+b)/2)
    #tsa2 = datalogger.load_tsa(datestring2)
    tsa2 = dataloggerweb.get_tsa(project, tablename, datestring2)
    tsa2 = datalogger.group_by(datestring2, tsa2, ("hostname",), lambda a,b : (a+b)/2)
    print "Duration load %f" % (time.time() - starttime)
    starttime = time.time()
    cm = CorrelationMatrixTime(tsa1, tsa2, value_key)
    print "TOP most differing keys between %s and %s" % (datestring1, datestring2)
    for key, coefficient in sorted(cm.items(), key=lambda items: items[1], reverse=True)[:20]:
        print key, coefficient

def report(project, tablename, datestring1, datestring2, value_key):
    # get data, from datalogger, or dataloggerhelper
    #datalogger = DataLogger(BASEDIR, project, tablename)
    dataloggerweb = DataLoggerWeb(DATALOGGER_URL)
    print "loading data"
    starttime = time.time()
    #tsa1 = datalogger.load_tsa(datestring1)
    tsa1 = dataloggerweb.get_tsa(project, tablename, datestring1)
    #tsastat1 = datalogger.load_tsastats(datestring1)
    tsastat1 = dataloggerweb.get_tsastats(project, tablename, datestring1)
    #tsa2 = datalogger.load_tsa(datestring2)
    tsa2 = dataloggerweb.get_tsa(project, tablename, datestring2)
    #tsastat2 = datalogger.load_tsastats(datestring2)
    tsastat2 = dataloggerweb.get_tsastats(project, tablename, datestring2)
    print "Duration load %f" % (time.time() - starttime)
    starttime = time.time()
    cm = CorrelationMatrixTime(tsa1, tsa2, value_key)
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
    report_group("vicenter", "virtualMachineCpuStats", datestring, date2.isoformat(), "cpu.used.summation")
    #report(project, tablename, datestring, date2.isoformat(), "mem.active.average")
    #report("vicenter", "virtualMachineDatastoreStats", datestring, date2.isoformat(), "datastore.totalReadLatency.average")
    #report("vicenter", "virtualMachineDatastoreStats", datestring, date2.isoformat(), "datastore.write.average")
    #report("vicenter", "virtualMachineNetworkStats", datestring, date2.isoformat(), "net.usage.average")
    #report("sanportperf", "fcIfC3AccountingTable", datestring, date2.isoformat(), "fcIfC3InOctets")

if __name__ == "__main__":
    main()
    #cProfile.run("main()")
