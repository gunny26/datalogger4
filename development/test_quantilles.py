#!/usr/bin/pypy
import cProfile
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(levelname)s %(filename)s:%(funcName)s:%(lineno)s %(message)s')
from datalogger import DataLogger as DataLogger
from commons import *

class Quantilles(object):
    """
    class to calulate and store quantilles for one specific timeseries value_key
    """

    def __init__(self, tsa, value_key, maxx=None):
        self.__quantilles = {}
        self.__sortlist = None
        if maxx is None:
            self.maxx = max((max(ts[value_key]) for key, ts in tsa.items()))
        else:
            self.maxx = maxx
        for key, ts in tsa.items():
            self.__quantilles[key] = self.calculate(ts[value_key])
        self.sort()

    def calculate(self, series):
        """
        actually do the calculations
        """
        quants = {
            0 : 0,
            1 : 0,
            2 : 0,
            3 : 0,
            4 : 0,
        }
        width = int(100 / (len(quants.keys()) -1))
        for value in series:
            quant = int((100 * min(value, self.maxx)/self.maxx) / width)
            quants[quant] += 1
        return quants

    def head(self, maxlines=10):
        """
        output head
        """
        outbuffer = []
        for index, key in enumerate(self.__sortlist[::-1]):
            outbuffer.append("%s : %s" % (str(key), str(self.__quantilles[key])))
            if index == maxlines:
                break
        return "\n".join(outbuffer)

    def tail(self, maxlines=10):
        """
        output tail
        """
        outbuffer = []
        for index, key in enumerate(self.__sortlist):
            outbuffer.append("%s : %s" % (str(key), str(self.__quantilles[key])))
            if index == maxlines:
                break
        return "\n".join(outbuffer)

    def __str__(self):
        """
        combine head and tail
        """
        outbuffer = []
        outbuffer.append("%d keys in dataset" % len(self.__quantilles))
        outbuffer.append(self.head())
        outbuffer.append("...")
        outbuffer.append(self.tail())
        return "\n".join(outbuffer)

    def sort(self):
        """
        sort on one specific quantile
        """
        self.__sortlist = []
        for key, values in sorted(self.__quantilles.items(), key=lambda items: sum((10^quantille * count for quantille, count in enumerate(items[1].values())))):
            self.__sortlist.append(key)


def report(datalogger, datestring):
    # get data, from datalogger, or dataloggerhelper
    tsa = datalogger.load_tsa(datestring)
    # tsa_test = tsa.slice(("cpu.used.summation", ))
    quantilles = Quantilles(tsa, "cpu.used.summation", maxx=20000.0)
    print quantilles

def main():
    project = "vicenter"
    tablename = "virtualMachineCpuStats"
    datalogger = DataLogger(BASEDIR, project, tablename)
    datestring = get_last_business_day_datestring()
    report(datalogger, datestring)

if __name__ == "__main__":
    #main()
    cProfile.run("main()")
