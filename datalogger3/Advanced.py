#!/usr/bin/python3
import json
# own modules
from datalogger3.DataLogger import DataLogger as DataLogger
from datalogger3.TimeseriesArray import TimeseriesArray as TimeseriesArray
from datalogger3.TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats

def tsa_group_by(tsa, datestring, index_keynames, group_func, interval):
    """
    group given tsa by subkeys, and use group_func to aggregate data
    the individual timeseries are automatically grouped by timestamp and interval
    defined in configuration

    parameters:
    tsa <TimeseriesArray>
    datestring <str> datestring to use to aggregate data TODO: get this from tsa
    subkey <tuple> could also be empty, to aggregate everything
    group_func <func> like lambda a, b : (a + b) / 2 to get averages
    interval <int> interval in seconds the timeseries values should appear

    returns:
    <TimeseriesArray>
    """
    # intermediated tsa
    tsa2 = TimeseriesArray(index_keynames=index_keynames, value_keynames=tsa.value_keynames, ts_key=tsa.ts_key, datatypes=tsa.datatypes)
    start_ts, _ = DataLogger.get_ts_for_datestring(datestring)
    ts_keyname = tsa.ts_key
    for data in tsa.export():
        # align timestamp
        nearest_slot = round((data[ts_keyname] - start_ts) / interval)
        data[ts_keyname] = int(start_ts + nearest_slot * interval)
        tsa2.add(data, group_func)
    return tsa2

def tsastats_group_by(tsastat, index_keynames):
    """
    group given tsastat array by some subkey

    parameters:
    tsastat <TimeseriesArrayStats>
    subkey <tuple> subkey to group by

    returns:
    <dict>
    """
    # how to aggregate statistical values
    group_funcs = {
        u'count' : lambda a, b: a + b,
        u'std' : lambda a, b: (a + b)/2,
        u'avg': lambda a, b: (a + b)/2,
        u'last' : lambda a, b: -1.0, # theres no meaning
        u'min' : min,
        u'max' : max,
        u'sum' : lambda a, b: (a + b) / 2,
        u'median' : lambda a, b: (a + b)/2,
        u'mean' : lambda a, b: (a + b)/2,
        u'diff' : lambda a, b: (a + b)/2,
        u'dec' : lambda a, b: (a + b)/2,
        u'inc' : lambda a, b: (a + b)/2,
        u'first' : lambda a, b: -1.0, # theres no meaning
    }
    # create new empty TimeseriesArrayStats Object
    #tsastats_new = TimeseriesArrayStats.__new__(TimeseriesArrayStats)
    #tsastats_new.index_keynames = index_keynames # only subkey
    #tsastats_new.value_keynames = tsastat.value_keynames # same oas original
    newdata = {}
    for index_key, tsstat in tsastat.items():
        key_dict = dict(zip(tsastat.index_keynames, index_key))
        newkey = None
        if len(index_keynames) == 0: # no subkey means total aggregation
            newkey = ("__total__", )
        else:
            newkey = tuple([key_dict[key] for key in index_keynames])
        if newkey not in newdata:
            newdata[newkey] = {}
        for value_keyname in tsastat.value_keynames:
            if value_keyname not in newdata[newkey]:
                newdata[newkey][value_keyname] = dict(tsstat[value_keyname])
            else:
                for stat_funcname in tsstat[value_keyname].keys():
                    existing = float(newdata[newkey][value_keyname][stat_funcname])
                    to_group = float(tsstat[value_keyname][stat_funcname])
                    newdata[newkey][value_keyname][stat_funcname] = group_funcs[stat_funcname](existing, to_group)

    tsastats_data = [
        index_keynames,
        tsastat.value_keynames,
        [(key, data) for key, data in newdata.items()]
    ]
    tsastats = TimeseriesArrayStats.from_json(json.dumps(tsastats_data))
    return tsastats

def get_scatter_data(tsa, value_keynames, stat_func):
    """
    get data structure to use for highgraph scatter plots,
    [
        {
            name : str(<key>),
            data : [stat_func(tsa[key][value_keys[0]]), stat_func(tsa[key][value_keys[1]], ]
        },
        ...
    ]

    parameters:
    tsa <TimeseriesArray>
    value_keys <tuple> with len 2, represents x- and y-axis
    stat_fuc <str> statistical function to use to aggregate xcolumn and ycolumns
        must exist in Timeseries object

    returns:
    <list> of <dict> data structure to use directly in highgraph scatter plots, when json encoded
    """
    assert len(value_keynames) == 2
    data = []
    for key in tsa.keys():
        stats = tsa[key].get_stat(stat_func)
        data.append([key, stats[value_keynames[0]], stats[value_keynames[1]]])
    return data
