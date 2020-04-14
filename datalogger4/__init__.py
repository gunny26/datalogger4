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
__version__ = "$Revision: 4.0.0 $"
print("running __init__")
from .DataLogger import DataLogger as DataLogger
from .Timeseries import Timeseries as Timeseries
from .TimeseriesArray import TimeseriesArray as TimeseriesArray
from .TimeseriesStats import TimeseriesStats as TimeseriesStats
from .TimeseriesArrayStats import TimeseriesArrayStats as TimeseriesArrayStats
from .Quantile import QuantileArray as QuantileArray
from .Quantile import Quantile as Quantile
from .CorrelationMatrix import CorrelationMatrixArray as CorrelationMatrixArray
from .CorrelationMatrixTime import CorrelationMatrixTime as CorrelationMatrixTime
from .FastTsa import fast_tsa
from .b64 import b64eval, b64encode, b64decode
# custom exceptions
from .CustomExceptions import *
