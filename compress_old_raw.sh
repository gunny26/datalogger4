#!/bin/bash

find /var/rrd/ -name "*.csv" -type f -mtime +0 -exec gzip {} \;
