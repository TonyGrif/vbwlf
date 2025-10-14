"""This module contains code for interacting with USGS data"""

from .parser import query_instantaneous_values, parse_instantaneous_values

__all__ = ["query_instantaneous_values", "parse_instantaneous_values"]
