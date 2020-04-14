# datalogger
system to store and analyze timeseries data in a huge manner

no dependency to any type of database, all stored on local or remote filesystem
accessible through python module, or RestFul Web API

## project

main order item, like vicenter, snmp, haproxy

## tablename

definition of input data, beneath a  project

## Timeseries class

Stores a bunch of metrics about one item, e.g. for one instance of virtual cpu

cpu.used.summary
cpu.idle.summary
cpu.ready.summary

all three (in this case) value_keys must be strictly numeric, and are stored as float

every Timeseries must have exactly one timestamp column to very set of value_keys

## TimeseriesArray class

holding a bunch of Timeseries seperated by different index_keys

## TimeseriesStatsArray

holding a bunch of different TimeseriesStats seperated by different index_keys

## TimeseriesStats

automatically calculate some statistical values for some Timeseries, like min/max/avg/std/...
