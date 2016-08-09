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
import sys
import time
import datetime
import json
import logging
logging.basicConfig(level=logging.INFO)
import logging.handlers
from optparse import OptionParser
# eigene Module
import tilak_cmdb
import tilak_mysql
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
    and privilegiert=1
    and passwort is not NULL
    and upper(dienst)='mysql'
    order by Host desc
"""

SQL_SPACE_STRING = u"""
SELECT
    table_schema "db_name",
    count(table_name) "db_table_count",
    sum(table_rows) "db_table_rows",
    sum( data_length) "db_data_size_kb",
    sum( index_length ) "db_index_size_kb",
    sum( data_length + index_length ) "db_size_kb",
    sum( data_free ) "db_free"
FROM information_schema.TABLES
GROUP BY table_schema
"""

BASEDIR = u"/var/rrd"
PROJECT = u"mysql"
TABLENAME = u"space"
TS_KEYNAME = u"ts"
DELIMITER = u"\t"
INDEX_KEYNAMES = (u"hostname", u"db_name")
VALUE_KEYNAMES = (u"db_table_rows", u"db_data_size", u"db_index_size", u"db_size", u"db_free")
HEADERS = (TS_KEYNAME, ) + INDEX_KEYNAMES + VALUE_KEYNAMES

METADATA = {
    "blacklist": [],
    "ts_keyname": TS_KEYNAME,
    "headers": HEADERS,
    "delimiter": DELIMITER,
    "value_keynames": {
        "db_table_rows" : "asis",
        "db_data_size" : "asis",
        "db_index_size" : "asis",
        "db_size" : "asis",
        "db_free" : "asis"
    },
    "index_keynames": INDEX_KEYNAMES,
    "interval" : 300
}

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
        mc = tilak_mysql.MySqlConnection(row.host, row.username, row.passwort)
        try:
            con = mc.getConnection()
            cur = con.cursor()
            cur.execute(SQL_SPACE_STRING)
            ts = time.time()
            hostname = unicode(row.host)
            for data in cur.fetchall():
                rowdata = (unicode(ts), hostname) + tuple((unicode(col) for col in data))
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
    (options, args)=parser.parse_args()
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
        json.dump(METADATA, open(os.path.join(meta_dir, "%s.json" % TABLENAME), "wb"))
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
