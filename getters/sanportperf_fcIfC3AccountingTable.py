#!/usr/bin/python
import logging
logging.basicConfig(level=logging.ERROR)
import os
#import tilak_snmp
import time
import datetime
import subprocess
import tilak_centreon

def get_snmp_table(hostname, table_oid, community, index=False):
    """
    hostname : <str>
    table_oid : <str>
    community : <str>
    index : <bool> append index to every row, snmptable Option -Ci

    call snmptable command and get output

    ooutput will be transferred to list of dictionaries,
    key names are taken from output at line 3

    to function properly, the MIB of the mentioned table must be present and installed,
    under ubuntu  use user specific diectory under ~/.snmp/mibs to store vendor specific files

    every dataset - aka row of data - is prependet with key "hostname" and "ts" : timestamp of call
    """
    # field should be extra separated, not the default space
    cmd = ""
    if index is False:
        cmd = "snmptable -v2c -c %s -Cf \; %s %s" % (community, hostname, table_oid)
    else:
        cmd = "snmptable -v2c -c %s -Ci -Cf \; %s %s" % (community, hostname, table_oid)
    logging.info(cmd)
    output = subprocess.check_output((cmd, ), shell=True)
    lines_to_ignore = 1 # ignore first two line
    header_line = True # next is header line
    headers = [] # headers are stored in list
    data = [] # result
    keys = {
        "hostname" : hostname,
        "ts" : time.time()
    }
    for line in output.split("\n"):
        if line == "":
            continue # ignore blank lines
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
    return data

def get_fcIfC3AccountingTable(hostname, community):
    if_data = get_snmp_table(hostname, "IF-MIB:ifTable" , community, index=True)
    ifindex_to_descr = {}
    for row in if_data:
        ifindex_to_descr[row["index"]] = row["ifDescr"]
    # inject ifDescr to every row
    fc_data = get_snmp_table(hostname, "CISCO-FC-FE-MIB:fcIfC3AccountingTable" , community, index=True)
    for row in fc_data:
        row["ifDescr"] = ifindex_to_descr[row["index"]]
    return(fc_data)

def save_data(filename, data):
    """
    save data to file
    input data should be a list, containing dictionaries of row data

    [
        { key1 : value1,
          key2 : value2,
        },
        { key1 : value1,
          key2 : value2,
        },
    ]

    if file already exists, the new data is appended.

    if the file will be created, the firstline will be header names, sorted

    all row data will be written in sorted keys ranking
    """
    if len(data) > 0:
        outfile = None
        if not os.path.exists(filename):
            outfile = open(filename, "wb")
            outfile.write("\t".join(sorted(data[0].keys())))
            outfile.write("\n")
        else:
            outfile = open(filename, "ab")
        for row in data:
            outfile.write("\t".join((str(row[k]) for k in sorted(row.keys()))))
            outfile.write("\n")
        outfile.close()
    else:
        print "No Data returned"

if __name__ == "__main__":
    centreon = tilak_centreon.Centreon()
    project = "sanportperf"
    tablename = "fcIfC3AccountingTable"
    nagios_group = "HW_CISCO_FC_SWITCHES"
    basedir = os.path.join("/var/rrd", project)
    if not os.path.exists(basedir):
        os.mkdir(basedir)
    basedir_raw = os.path.join(basedir, "raw")
    basedir_rrd = os.path.join(basedir, "rrd")
    basedir_gfx = os.path.join(basedir, "gfx")
    for directory in (basedir_raw, basedir_rrd, basedir_gfx):
        if not os.path.exists(directory):
            os.mkdir(directory)
    datestring = datetime.date.today().isoformat()
    csv_filename = os.path.join(basedir_raw, "%s_%s.csv" % (tablename, datestring))
    # get switchnames from centreon database
    for row in centreon.getCentreonHostGroupMembersSnmp(nagios_group):
        hostname, community, version = row
        logging.info("Getting Data from %s", hostname)
        data = get_fcIfC3AccountingTable(hostname.split(".")[0], community)
        save_data(csv_filename, data)
