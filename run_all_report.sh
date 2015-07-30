#!/bin/bash

cd /root/datalogger
for report in `ls report*.py`; do 
    python $report > /var/log/$report.log
done
