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
Generate Tootal Stats for every Table
"""
__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2008 Arthur Messner"
__license__ = "GPL"
import sys
import os
import datetime
import argparse
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("requests").setLevel(logging.CRITICAL)
import json
# own modules
from tk_webapis import DataLoggerWebClient as DataLoggerWebClient


def get_stat_total(project, tablename, datestring):
    """
    calculate total statistic for one particular project/tablename/datestring combination

    project <basestring>
    tablename <basestring>
    datestring <basestring>

    return dictionary of stats
    """
    aggregator = {
        'median': lambda a, b: 0.0,
        'avg': lambda a, b: a + b,
        'last': lambda a, b: 0.0,
        'diff': lambda a, b: 0.0,
        'max': lambda a, b: max(a, b),
        'first': lambda a, b: 0.0,
        'min': lambda a, b: min(a, b),
        'std': lambda a, b: 0.0,
        'count': lambda a, b: a + b,
        'mean': lambda a, b: 0.0,
        'dec': lambda a, b: a + b,
        'inc': lambda a, b: a + b,
        'sum': lambda a, b: a + b,
        'total_count' : lambda a, b: a # to be consistent
    }
    assert project in dl.get_projects()
    assert tablename in dl.get_tablenames(project)
    tsastat = dl.get_tsastat(project, tablename, datestring)
    index_keyname, value_keynames, data = tsastat
    stats_data = {}
    for value_keyname in value_keynames:
        stats_data[value_keyname] = dict((key, 0.0) for key in aggregator.keys())
        for ts_stat in data:
            index_key, stats = ts_stat
            for stat_func in stats[value_keyname].keys():
                stats_data[value_keyname][stat_func] = aggregator[stat_func](stats_data[value_keyname][stat_func], stats[value_keyname][stat_func])
            stats_data[value_keyname]["total_count"] += 1
        if stats_data[value_keyname]["total_count"] > 0:
            stats_data[value_keyname]["total_avg"] = stats_data[value_keyname]["sum"] / stats_data[value_keyname]["total_count"]
            stats_data[value_keyname]["avg"] /= stats_data[value_keyname]["total_count"]
        else:
            stats_data[value_keyname]["total_avg"] = 0.0
            stats_data[value_keyname]["avg"] = 0.0
    return stats_data

if __name__ == "__main__":
    yesterday_datestring = (datetime.date.today() - datetime.timedelta(1)).isoformat()
    parser = argparse.ArgumentParser(description="generate total statistics for DataLogger tables")
    parser.add_argument('--basedir', default="/var/rrd", help="basedirectory of datalogger data on local machine, default : %(default)s")
    parser.add_argument("-b", '--back', help="how many days back from now")
    parser.add_argument("-s", '--startdate', help="start date in isoformat YYYY-MM-DD")
    parser.add_argument("-e", '--enddate', default=yesterday_datestring, help="stop date in isoformat YYYY-MM-DD, default : %(default)s")
    parser.add_argument("-q", '--quiet', action='store_true', help="set to loglevel ERROR")
    parser.add_argument("-v", '--verbose', action='store_true', help="set to loglevel DEBUG")
    parser.add_argument("-p", '--project', help="process only this project name")
    parser.add_argument("-t", '--tablename', help="process only this tablename")
    parser.add_argument("-f", '--force', action="store_true", help="force recreateion, otherwise skip if file already exists")
    # options = parser.parse_args("-b 2".split())
    options = parser.parse_args()
    if options.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if options.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    if (options.back is not None) == (options.startdate is not None):
        logging.error("option -b and -e are mutual exclusive, use only one")
        sys.exit(1)
    startdate = None
    if options.back is not None:
        startdate = (datetime.date.today() - datetime.timedelta(int(options.back))).isoformat()
    elif options.startdate is not None:
        startdate = options.startdate
    else:
        logging.error("you have to provide either -b or -s")
        sys.exit(1)
    # start your engines
    dl = DataLoggerWebClient()
    for datestring in dl.datewalker(startdate, options.enddate):
        for project in dl.get_projects():
            if (options.project is not None) and (project != options.project):
                continue
            for tablename in dl.get_tablenames(project):
                if (options.tablename is not None) and (tablename != options.tablename):
                    continue
                subdir = os.path.join(options.basedir, "global_cache", datestring, project, tablename)
                if not os.path.isdir(subdir):
                    logging.error("directory %s does not exist" % subdir)
                    continue
                logging.info("analyzing %s/%s/%s" % (project, tablename, datestring))
                try:
                    filename = os.path.join(subdir, "total_stats.json")
                    if (options.force is False) and (os.path.isfile(filename)):
                        logging.info("skipping, data already available")
                        continue
                    logging.info("storing data to %s", filename)
                    stats_data = get_stat_total(project, tablename, datestring)
                    json.dump(stats_data, open(filename, "wt"), indent=4)
                    if options.verbose is True:
                        for value_keyname in dl.get_meta(project, tablename)["value_keynames"]:
                            logging.debug("%0.2f\t%0.2f\t%0.2f\t%d\t%s" % (stats_data[value_keyname]["avg"], stats_data[value_keyname]["total_avg"], stats_data[value_keyname]["sum"], stats_data[value_keyname]["total_count"], value_keyname))
                except KeyError as exc:
                    logging.info(exc)
                    logging.error("%s/%s/%s skipping, no tsastat data available", project, tablename, datestring)
