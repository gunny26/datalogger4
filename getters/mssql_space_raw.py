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
get space informations from MS-SQL Database Instances and push this data
to csv for further use with datalogger
"""

__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2008 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision: 1.9 $"
__date__ = "$Date: 2013/12/13 12:09:07 $"
# $Id: check_mssql_space.py,v 1.9 2013/12/13 12:09:07 cvs_systembetrieb Exp $

import os
import datetime
import pymssql
import time
import logging
logging.basicConfig(level=logging.ERROR)
# own modules
import tk_webapis


def get_connection(username, password, hostname, connection_string, database="master"):
    """
    return connection object for database
    """
    try:
        con = None
        if (connection_string is not None) and (len(connection_string) > 0):
            logging.debug("connection string given: %s", connection_string)
            con = pymssql.connect(
                host=r"%s" % connection_string,
                user=username,
                password=password,
                database=database)
        else:
            con = pymssql.connect(
                host=hostname,
                user=username,
                password=password,
                database=database)
        return con
    except Exception as exc:
        logging.error("Connection Failure to %s", hostname)
        logging.exception(exc)
        raise exc

def get_online_databases(check_cur):
    """
    returnes list of online database of this connection
    """
    databases = []
    check_cur.execute("select name, status from sysdatabases")
    for row in check_cur.fetchall():
        #print row
        # if bit 512 is set, database is offline
        if int(row[1]) & 512 == 0:
            databases.append(row[0].lower())
        else:
            logging.info("database %s is offline", row[0])
    return databases

def get_credentials(cmdb_con, server):
    """
    get credentials from credentials store
    """
    sqlstring = """
    select
        username,
        passwort,
        host
    from tbl_logins, tbl_server
    where
        lower(username)='sa'
    and lower(dienst)='mssql'
    and tbl_server.hostname='%s'
    and tbl_logins.fk_server=tbl_server.id
    and tbl_logins.deleted=0
    order by hostname"""
    cur = cmdb_con.cursor()
    cur.execute(sqlstring % server)
    data = cur.fetchall()
    if len(data) == 0:
        logging.error("No credentials for host %s available", server)
        raise StandardError("No credentials found")
    username, password, connection_string = data[0]
    if password is None:
        password = ""
    return username, password, connection_string

def get_data():
    """ simply main """
    credentialstore = tk_webapis.CredentialStoreWebClient()
    centreonweb = tk_webapis.CentreonWebClient()
    servers = centreonweb.get_hostgroups("CMDB_MSSQL")["hosts"]
    logging.debug(servers)
    outdata = {}
    for server in servers:
        logging.info("working on server %s ", server)
        # get login to this server
        credentials = credentialstore.search({"authority": server, "user":"sa", "schema":"mssql"})
        # take only first entry
        cred = credentials[0]
        check_con = get_connection(cred["user"], cred["password"], server, cred["path"])
        check_cur = check_con.cursor()
        # get instance name
        check_cur.execute("select SERVERPROPERTY('InstanceName')")
        instance = None
        for row in check_cur.fetchall():
            instance = str(row[0])
        # build list of databases to analyze
        databases = get_online_databases(check_cur)
        # analyze every database in list
        for database in databases:
            check_cur.execute("use \"%s\"" % database)
            check_cur.execute("exec sp_helpfile")
            for row in check_cur.fetchall():
                key = (server, instance, database, row[7])
                used = int(row[4].split(" ")[0])
                maxsize = None
                # convert Unlimited text string to -1
                if row[5] == "Unlimited":
                    maxsize = -1
                else:
                    maxsize = int(row[5].split(" ")[0])
                # accumulate usage and maxsize
                if key not in outdata:
                    outdata[key] = {
                        "ts" : time.time(),
                        "used_kb" : used,
                        "maxsize_kb" : maxsize
                    }
                else:
                    outdata[key]["used_kb"] += used
                    if (outdata[key]["maxsize_kb"] == -1) or (maxsize == -1):
                        outdata[key]["maxsize_kb"] = 1
                    else:
                        outdata[key]["maxsize_kb"] += maxsize
                #print key, used, maxsize
        check_con.close()
    return outdata


def main():
    """
    create directory structure,
    get data
    output data to file
    """
    basedir = "/var/rrd/"
    project = "mssql"
    tablename = "space"
    # create dircetory structures
    project_dir = os.path.join(basedir, project)
    if not os.path.isdir(project_dir):
        os.mkdir(project_dir)
    raw_dir = os.path.join(project_dir, "raw")
    if not os.path.isdir(raw_dir):
        os.mkdir(raw_dir)
    datestring = datetime.date.today().isoformat()
    # build output filename
    tablefilename = os.path.join(raw_dir, "%s_%s.csv" % (tablename, datestring))
    # get csv like data
    outdata = get_data()
    # define data structure
    ts_keyname = "ts"
    index_keynames = ("hostname", "instance", "database", "usage")
    value_keynames = ("used_kb", "maxsize_kb")
    headers = tuple([ts_keyname, ] + list(index_keynames) + list(value_keynames))
    # output to file
    outbuffer = []
    fh = None
    if not os.path.exists(tablefilename):
        # print header only if this file will be created
        outbuffer.append("\t".join(headers))
        fh = open(tablefilename, "w")
    else:
        fh = open(tablefilename, "a")
    for key, values in outdata.items():
        rowdict = dict(zip(index_keynames, key))
        rowdict.update(values)
        outbuffer.append("\t".join((str(rowdict[keyname]) for keyname in headers)))
    fh.write("\n".join(outbuffer))
    fh.write("\n") # trailing newline
    fh.close()

if __name__ == "__main__":
    main()

