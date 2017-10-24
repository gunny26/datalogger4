#!/usr/bin/python
import logging
logging.basicConfig(level=logging.ERROR)
import os
#import tilak_snmp
import time
import datetime
import subprocess


def get_snmpTable(hostname, table_oid, community):
    # field should be extra separated, not the default space
    output = subprocess.check_output(("snmptable -v2c -c %s -Cf \; %s %s" % (community, hostname, table_oid)), shell=True)
    lines_to_ignore = 1 # ignore first two line
    header_line = True # next is header line
    headers = [] # headers are stored in list
    data = [] # result
    keys = {
        "hostname" : hostname,
        "ts" : time.time()
    }
    index = 0
    for line in output.split("\n"):
        if line == "":
            continue
        if lines_to_ignore > 0:
            lines_to_ignore -= 1
            continue
        else:
            if header_line is True:
                headers = line.strip().split(";")
                header_line = False
            else:
                subindex = 0
                values = keys.copy()
                for col in line.strip().split(";"):
                    values[headers[subindex]] = col.replace("\"", "")
                    subindex += 1
                data.append(values)
                index += 1
    return data

def save_data(filename, snmpResult):
    if len(snmpResult) > 0:
        fd = None
        if not os.path.exists(filename):
            fd = open(filename, "wb")
            fd.write("\t".join(sorted(snmpResult[0].keys())))
            fd.write("\n")
        else:
            fd = open(filename, "ab")
        for data in snmpResult:
            fd.write("\t".join((str(data[k]) for k in sorted(data.keys()))))
            fd.write("\n")
        fd.close()
    else:
        print "No Data returned"

servers = ("vsanapp1", "vsanapp2", "vsanapp3", "vsanapp4", "vsanapp5", "vsanapp6")
basedir = "/var/rrd/ipstor/raw"
datestring = datetime.date.today().isoformat()
for server in servers:
    filename = "vrsClientTable_%s.csv" % datestring
    save_data(os.path.join(basedir, filename), get_snmpTable(server, "IPSTOR-MIB:vrsClientTable", community="public"))
    filename = "vrsRealDevTable_%s.csv" % datestring
    save_data(os.path.join(basedir, filename), get_snmpTable(server, "IPSTOR-MIB:vrsRealDevTable", community="public"))
