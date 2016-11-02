#!/usr/bin/env python
# pylint: disable=line-too-long
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

""" Packet to work with Datalogger System"""

__author__ = "Arthur Messner <arthur.messner@gmail.com>"
__copyright__ = "Copyright (c) 2008 Arthur Messner"
__license__ = "GPL"
__version__ = "$Revision: 1.2 $"
__date__ = "$Date: 2013/04/16 06:50:15 $"
# $Id: __init__.py,v 1.2 2013/04/16 06:50:15 mesznera Exp $

from datalogger.DataLogger import DataLogger as DataLogger
from datalogger.DataLogger import DataLoggerRawFileMissing as DataLoggerRawFileMissing
from datalogger.Timeseries import Timeseries as Timeseries
from datalogger.TimeseriesArrayLazy import TimeseriesArrayLazy as TimeseriesArray
from datalogger.TimeseriesStats import TimeseriesStats as TimeseriesStats
from datalogger.TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats
from datalogger.Quantile import QuantileArray as QuantileArray
from datalogger.Quantile import Quantile as Quantile
from datalogger.CorrelationMatrix import CorrelationMatrixArray as CorrelationMatrixArray
from datalogger.CorrelationMatrixTime import CorrelationMatrixTime as CorrelationMatrixTime
# custom exceptions
from datalogger.CustomExceptions import TimeseriesEmptyError as TimeseriesEmptyError
from datalogger.CustomExceptions import DataLoggerRawFileMissing as DataLoggerRawFileMissing
from datalogger.CustomExceptions import DataLoggerLiveDataError as DataLoggerLiveDataError
from datalogger.CustomExceptions import DataLoggerFilenameDecodeError as DataLoggerFilenameDecodeError
# classes to use with webapplication API
from datalogger.DataLoggerWeb import DataLoggerWeb as DataLoggerWeb
from datalogger.DataLoggerRest import DataLoggerRest as DataLoggerRest
from datalogger.CorrelationMatrixTime import CorrelationMatrixTimeWeb as CorrelationMatrixTimeWeb
