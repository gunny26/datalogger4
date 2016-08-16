#!/usr/bin/env python
#
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
"""
Program to check all stored mysql-server root accounts in tbl_logins
"""

__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__version__ = "0.1"
__date__ = "Date: 21.04.2008"
__copyright__ = "Copyright (c) 2008 Arthur Messner"
__license__ = "GPL"

import os
import time
import datetime
import json
import MySQLdb
import logging
logging.basicConfig(level=logging.INFO)
import logging.handlers
from optparse import OptionParser
# eigene Module
import tilak_cmdb
import tilak_centreon

NAGIOS_GROUP = u"CHECK_MYSQL_LOGIN"

SQL_CREDENTIALS = u"""
    select
        username,
        passwort,
        dienst,
        lower(tbl_server.hostname),
        host
    from tbl_logins, tbl_server
    where tbl_logins.fk_server=tbl_server.id
    and upper(username)='ROOT'
    and fk_server is not NULL
    and tbl_server.deleted=0
    and passwort is not NULL
    and upper(dienst)='mysql'
    order by Host desc
"""

BASEDIR = u"/var/rrd"
PROJECT = u"mysql"
TABLENAME = u"performance"
TS_KEYNAME = u"ts"
DELIMITER = u"\t"
INDEX_KEYNAMES = (u"hostname", )
METADATA = {
    "blacklist": [],
    "ts_keyname": TS_KEYNAME,
    "delimiter": DELIMITER,
    "value_keynames": {
        "bytes_received" : "persecond",
        "bytes_sent" : "persecond",
        "questions" : "persecond",
        "uptime" : "asis",
        "slow_queries" : "asis",
        "opened_tables" : "asis",
        "qcache_hits" : "persecond",
        "aborted_clients" : "persecond",
        "aborted_connects" : "persecond",
        "created_tmp_disk_tables" : "persecond",
        "created_tmp_tables" : "persecond",
        "com_select" : "persecond",
        "com_insert" : "persecond",
        "com_delete" : "persecond",
        "com_update" : "persecond",
        "connections" : "persecond"
    },
    "index_keynames": INDEX_KEYNAMES,
    "interval" : 300
}
VALUE_KEYNAMES = tuple(METADATA["value_keynames"].keys())
METADATA["headers"] = (TS_KEYNAME, ) + INDEX_KEYNAMES + VALUE_KEYNAMES
HEADERS = METADATA["headers"]


def main():
    """
    get credentials from tbl_logins for all stored mysql servers
    then iterate and test every login
    accoridng to result send nagios message for this particular server

    if any credential fails, send one nagios message to the host running this script
    """
    cmdb = tilak_cmdb.Cmdb()
    centreon = tilak_centreon.Centreon()
    nagios_hosts = centreon.getCentreonHostGroupMembers(NAGIOS_GROUP)
    logging.debug(SQL_CREDENTIALS)
    outdata = []
    for row in cmdb.getCredentials(SQL_CREDENTIALS):
        if row.host not in nagios_hosts:
            continue
        else:
            nagios_hosts.remove(row.host)
        logging.debug(row.toString())
        logging.debug("Teste Server %s" % row.host)
        #mc = tilak_mysql.MySqlConnection(row.host, row.username, row.passwort)
        try:
            #con = mc.getConnection()
            con = MySQLdb.connect(row.host, row.username, row.passwort, charset="utf8")
            cur = con.cursor()
            #cur.execute("select * from information_schema.global_status")
            cur.execute("show global status")
            ts = time.time()
            hostname = unicode(row.host)
            # convert status table to dict
            status_dict = {}
            for data in cur.fetchall():
                status_dict[data[0].lower()] = data[1]
            rowdata = (unicode(ts), hostname) + tuple((status_dict[key] for key in VALUE_KEYNAMES))
            outdata.append(rowdata)
            logging.info(DELIMITER.join(rowdata))
        except StandardError as exc:
            logging.exception(exc)
    return outdata


if __name__ == "__main__":
    # parse commandline
    parser = OptionParser(usage="%prog [-v] [-q]", version=__version__)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", \
        default=False, help="verbose output")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true", \
        default=False, help="quiet")
    (options, args) = parser.parse_args()
    # set logging level according to commandline switches
    if options.quiet:
        logging.getLogger("").setLevel(logging.ERROR)
    elif options.verbose:
        logging.getLogger("").setLevel(logging.DEBUG)
    else:
        logging.getLogger("").setLevel(logging.INFO)

    # create dircetory structures
    project_dir = os.path.join(BASEDIR, PROJECT)
    if not os.path.isdir(project_dir):
        os.mkdir(project_dir)
    raw_dir = os.path.join(project_dir, "raw")
    if not os.path.isdir(raw_dir):
        os.mkdir(raw_dir)
    meta_dir = os.path.join(project_dir, "meta")
    if not os.path.isdir(meta_dir):
        os.mkdir(meta_dir)
    metafilename = os.path.join(meta_dir, "%s.json" % TABLENAME)
    if not os.path.isfile(metafilename):
        json.dump(METADATA, open(metafilename, "wb"))
    datestring = datetime.date.today().isoformat()
    # build output filename
    tablefilename = os.path.join(raw_dir, "%s_%s.csv" % (TABLENAME, datestring))
    # get csv like data
    outdata = main()
    # output to file
    outbuffer = []
    fh = None
    if not os.path.exists(tablefilename):
        # print header only if this file will be created
        outbuffer.append(DELIMITER.join(HEADERS))
        fh = open(tablefilename, "w")
    else:
        fh = open(tablefilename, "a")
    for row in outdata:
        outbuffer.append(DELIMITER.join(row))
    fh.write("\n".join(outbuffer))
    fh.write("\n") # trailing newline
    fh.close()
