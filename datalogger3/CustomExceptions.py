#!/usr/bin/python
# pylint: disable=line-too-long

class TimeseriesEmptyError(Exception):
    """raise if the specific Timeseries is length zero"""

class DataLoggerRawFileMissing(Exception):
    """raised, when there is no available Raw Input File"""

class DataLoggerLiveDataError(Exception):
    """raised if there is an attempt to read from live data"""

class DataLoggerFilenameDecodeError(Exception):
    """when the given filename is not decodable with base64"""

class QuantileError(Exception):
    """raised if there is some problem calculating Quantile"""

class DataFormatError(Exception):
    """raised if format does not match"""
