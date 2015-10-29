#!/usr/bin/python

class TimeseriesEmptyError(Exception):
    """raise if the specific Timeseries is length zero"""
    pass

class DataLoggerRawFileMissing(Exception):
    """raised, when there is no available Raw Input File"""
    pass

class DataLoggerLiveDataError(Exception):
    """raised if there is an attempt to read from live data"""
    pass

class DataLoggerFilenameDecodeError(Exception):
    """when the given filename is not decodable with base64"""
    pass


