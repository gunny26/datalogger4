#!/usr/bin/python3
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
generate some datalogger statistics from netcomm data
"""
import os
import logging
import datetime
import gzip
import io
# own modules
from tk_restapis import NetCommRestClient, DataLoggerRestClient

logging.basicConfig(level=logging.DEBUG)

def netcomm_to_datalogger(datestring):
    with NetCommRestClient(uri=uri, token_type="id_token", enable_stats=False) as client:
        header = "\t".join(("ts", "hostname", "total", "server", "client", "localhost", "pseudo_localhost", "public")) + "\n"
        yield header
        for hostname in client.get_hostnames(datestring):
            logging.info("analyzing %s/%s", datestring, hostname)
            data = client.get_connections(datestring, hostname)
            dl_histogram = {}
            for conn in data["connection_data"]:
                """
                {hostname: srvskimvic01.tilak.cc, tcpConnectionLocalAddress: 10.16.65.49, tcpConnectionLocalAddressType: ipv4,
                    tcpConnectionLocalPort: '56515', tcpConnectionRemAddress: 10.16.65.49, tcpConnectionRemAddressType: ipv4,
                    tcpConnectionRemPort: '80', tcpConnectionState: established, ts: '1567547516.6'}
                """
                if conn["ts"] not in dl_histogram:
                    dl_histogram[conn["ts"]] = {
                        "total": 0,
                        "server": 0,
                        "client": 0,
                        "localhost": 0,
                        "pseudo_localhost": 0,
                        "public": 0
                        }
                dl_histogram[conn["ts"]]["total"] += 1
                conn["datetime"] = datetime.datetime.fromtimestamp(float(conn["ts"])).isoformat()
                if conn["tcpConnectionLocalPort"] in data["listening_ports"]:
                    #print("%(datetime)s %(hostname)s %(tcpConnectionLocalAddress)s:%(tcpConnectionLocalPort)s <-  %(tcpConnectionRemAddress)s:%(tcpConnectionRemPort)s" % conn)
                    dl_histogram[conn["ts"]]["server"] += 1
                else:
                    #print("%(datetime)s %(hostname)s %(tcpConnectionLocalAddress)s:%(tcpConnectionLocalPort)s ->  %(tcpConnectionRemAddress)s:%(tcpConnectionRemPort)s" % conn)
                    dl_histogram[conn["ts"]]["client"] += 1
                if conn["tcpConnectionLocalAddress"].startswith("127") or conn["tcpConnectionRemAddress"].startswith("127"):
                    dl_histogram[conn["ts"]]["localhost"] += 1
                elif conn["tcpConnectionLocalAddress"] == conn["tcpConnectionRemAddress"]:
                    dl_histogram[conn["ts"]]["pseudo_localhost"] += 1
                else:
                    dl_histogram[conn["ts"]]["public"] += 1
            # print(yaml.dump(conn))
            for ts, counter in dl_histogram.items():
                ts_time = datetime.datetime.fromtimestamp(float(ts)).time()
                line = "\t".join((ts, hostname, str(counter["total"]), str(counter["server"]), str(counter["client"]), str(counter["localhost"]), str(counter["pseudo_localhost"]), str(counter["public"]))) + "\n"
                yield line

if __name__ == "__main__":
    uri = "https://rest-apis.tirol-kliniken.cc/netcomm/v1"
    with NetCommRestClient(uri=uri, token_type="id_token", enable_stats=False) as client:
        datestrings = list(client.get_datestrings())
    project = "netcomm"
    tablename = "connstat"
    for datestring in datestrings:
        dl_client = DataLoggerRestClient(uri="https://rest-apis.tirol-kliniken.cc/datalogger/v4", token_type="id_token", enable_stats=False)
        if not dl_client.exists(project, tablename, datestring):
            print("creating netcomm data for %s" % datestring)
            with io.StringIO() as outfile:
                for line in netcomm_to_datalogger(datestring):
                    outfile.write(line)
                print("uploading datalogger raw_file")
                dl_client.post_raw_file(project, tablename, datestring, outfile)
        else:
            print("dataloger data already present for %s" % datestring)

