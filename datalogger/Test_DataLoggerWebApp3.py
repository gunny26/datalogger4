#!/usr/bin/python2
from __future__ import print_function
import unittest
import logging
logging.basicConfig(level=logging.INFO)
import datetime
import gzip
import json
import os
# own modules
from DataLoggerWebApp3 import DataLoggerWebApp3 as DataLoggerWebApp3

class Test(unittest.TestCase):

    def setUp(self):
        self.dlw3 = DataLoggerWebApp3()

    def test_projects(self):
        print(self.dlw3.projects())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
