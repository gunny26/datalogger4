#!/usr/bin/python

import requests
import gzip
url = 'http://srvmgdata1.tilak.cc/DataLogger/upload_raw_file/test/fcIfC3AccountingTable/2015-09-01'
files = {'myfile': gzip.open("/var/rrd/sanportperf/raw/fcIfC3AccountingTable_2015-09-01.csv.gz")}
response = requests.post(url, files=files)
print response.text
