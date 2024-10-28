from __future__ import annotations

import datetime
from typing import Union, Any, Dict


class TimeSeriesDataProductFieldConfiguration:
    """
    Contains configurable (i.e. stateful, stored in db) metadata relating to a TimeSeriesDataProductField.
    The only currently-known example of this is an optional min and max value for validity-checking
    """
    effective_from: Union[datetime, None]
    effective_to: Union[datetime, None]
    min_valid_value: Union[Any, None]
    max_valid_value: Union[Any, None]
    lower_warn_threshold: Union[Any, None]
    upper_warn_threshold: Union[Any, None]

    def __init__(self,
                min_valid_value: Union[Any, None] = None, lower_warn_threshold: Union[Any, None] = None,
                upper_warn_threshold: Union[Any, None] = None, max_valid_value: Union[Any, None] = None,
                effective_from: Union[datetime, None] = None, effective_to: Union[datetime, None] = None):
        if effective_from is not None and effective_to is not None:
            if not effective_to > effective_from:
                raise ValueError(f'Cannot instantiate TimeSeriesDataProductFieldConfiguration where effective_to <= effective_from (got {effective_to}, {effective_from}, respectively)')

        self.effective_from = effective_from
        self.effective_to = effective_to
        self.min_valid_value = min_valid_value
        self.max_valid_value = max_valid_value
        self.lower_warn_threshold = lower_warn_threshold
        self.upper_warn_threshold = upper_warn_threshold

    @staticmethod
    def construct(product, field, effective_from: Union[datetime, None] = None, effective_to: Union[datetime, None] = None) -> TimeSeriesDataProductFieldConfiguration:
        """
        This is a stub to instantiate plausible min/max values for those types which are intended to be supported.
        TODO: In the future, this will fetch the values, if present, from the database.  It will be necessary to create
         a resolver factory method to allow instantiation of a collection of configuration objects with a single
         database query.
        """
        if field.python_type is int:
            return TimeSeriesDataProductFieldConfiguration(-1e10, -1e5, 1e5, 1e10, effective_from, effective_to)
        elif field.python_type is float:
            return TimeSeriesDataProductFieldConfiguration(float(-1e10), float(-1e5), float(1e5), float(1e10), effective_from, effective_to)
        elif field.python_type is datetime.datetime:
            return TimeSeriesDataProductFieldConfiguration(datetime.datetime.min, datetime.datetime.max, effective_from=effective_from, effective_to=effective_to)
        elif field.python_type is datetime.date:
            return TimeSeriesDataProductFieldConfiguration(datetime.date.min, datetime.date.max, effective_from=effective_from, effective_to=effective_to)
        else:
            raise ValueError(
                f'python type "{field.python_type.__name__}" not in supported python types (int, float, datetime, date)')

    def describe(self) -> Dict:
        return {
            'effective_from': self.effective_from,
            'effective_to': self.effective_to,
            'limits': None if (self.min_valid_value is None and self.max_valid_value is None) else {
                'lower': self.min_valid_value,
                'upper': self.max_valid_value
            },
            'warnings': None if (self.lower_warn_threshold is None and self.upper_warn_threshold is None) else {
                'lower': self.lower_warn_threshold,
                'upper': self.upper_warn_threshold
            }
        }