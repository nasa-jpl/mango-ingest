from __future__ import annotations

import datetime
from typing import Union, Any

class TimeSeriesDataProductFieldConfiguration:
    """
    Contains configurable (i.e. stateful, stored in db) metadata relating to a TimeSeriesDataProductField.
    The only currently-known example of this is an optional min and max value for validity-checking
    """
    min_valid_value: Union[Any, None]
    max_valid_value: Union[Any, None]

    def __init__(self, min_valid_value, max_valid_value):
        self.min_valid_value = min_valid_value
        self.max_valid_value = max_valid_value

    @staticmethod
    def construct(product, field) -> TimeSeriesDataProductFieldConfiguration:
        """
        This is a stub to instantiate plausible min/max values for those types which are intended to be supported.
        TODO: In the future, this will fetch the values, if present, from the database.  It will be necessary to create
         a resolver factory method to allow instantiation of a collection of configuration objects with a single
         database query.
        """
        if field.python_type is int:
            return TimeSeriesDataProductFieldConfiguration(-1e10, 1e10)
        elif field.python_type is float:
            return TimeSeriesDataProductFieldConfiguration(-1e10, float(1e10))
        elif field.python_type is datetime.datetime:
            return TimeSeriesDataProductFieldConfiguration(datetime.datetime.min, datetime.datetime.max)
        elif field.python_type is datetime.date:
            return TimeSeriesDataProductFieldConfiguration(datetime.date.min, datetime.date.max)
        else:
            raise ValueError(
                f'python type "{field.python_type.__name__}" not in supported python types (int, float, datetime, date)')
