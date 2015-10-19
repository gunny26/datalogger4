#!/bin/bash

cd /opt/datalogger/reports
for report in `ls report*.py`; do 
    python $report > /var/log/$report.log
done
