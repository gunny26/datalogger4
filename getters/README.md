Getter Scripts

their purpose is to get some arbitrary data from some source,
and write the data into datalogger raw file
usually this is done in CSV format delimited with <TAB>

these scripts are not related to datalogger at all,
there should be no import of some datalogger module.

Source System of any kind and data -> getter script -> RAW data written to file to use in datalogger

usually these script are run periodically

these script have to account a proper outputfile naming

<tablename>_<datestring>.csv

datestring should be calculated on received data
