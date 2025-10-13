from __future__ import annotations

import datetime
import json
import logging

from importlib import resources
from typing import Union, Any, List

#BEGIN STOPGAP - edunn 20251023
resource = resources.open_text('masschange.static', 'QUALITY_CHECKS.json')
content = json.load(resource)
#END STOPGAP

class ProductFieldConfiguration:
    """
    Contains configurable (i.e. stateful, stored in db) metadata relating to a TimeSeriesDataProductField.
    The only currently-known example of this is an optional min and max value for validity-checking
    """
    effective_since: Union[datetime, None]
    effective_until: Union[datetime, None]
    min_valid_value: Union[Any, None]
    max_valid_value: Union[Any, None]
    low_warning_value: Union[Any, None]
    high_warning_value: Union[Any, None]

    def __init__(self,
                 min_valid_value, max_valid_value,
                 low_warning_value, high_warning_value,
                 effective_since: Union[datetime, None] = None, effective_until: Union[datetime, None] = None
                 ):
        self.min_valid_value = min_valid_value
        self.max_valid_value = max_valid_value
        self.low_warning_value = low_warning_value
        self.high_warning_value = high_warning_value
        self.effective_since = effective_since
        self.effective_until = effective_until

    @staticmethod
    def construct_list(product, field) -> List[ProductFieldConfiguration]:
        """
        This is a stub to instantiate plausible min/max values for those types which are intended to be supported.
        TODO: In the future, this will fetch the values, if present, from the database.  It will be necessary to create
         a resolver factory method to allow instantiation of a collection of configuration objects with a single
         database query.

        TODO: Refactor all this, splitting the construction from the parsing of the static file (if database storage
         of configuration isn't immediately implemented or it's decided to use static json)
        """

        # TODO: This results in one access per call, when it should be cached, but working out how to invalidate the
        #  cache can wait until we implement the ability to update the JSON file.
        #  Probably just wrap the file in a class which checks the creation/modification timestamp every time it's
        #  accessed and updates itself if that timestamp has changed

        # resource, content moved to global scope as performance stopgap until above implemented - edunn 20251013
        # resource = resources.open_text('masschange.static', 'QUALITY_CHECKS.json')
        # content = json.load(resource)

        try:
            product_properties = content[product.id_suffix]['properties']
        except KeyError as err:
            logging.warning(f'Failed to resolve product "{product.id_suffix}" in "{resource.name}": {err}')
            return []

        try:
            property_thresholds = next(p for name, p in product_properties.items() if name == field.name)['thresholds']
        except (KeyError, StopIteration) as err:
            logging.warning(
                f'Failed to resolve property thresholds for "{field.name}" in product "{product.id_suffix}" in "{resource.name}": {err}')
            return []

        result = []
        for threshold_set in property_thresholds:
            effective_since = threshold_set.get('effective_since')
            effective_until = threshold_set.get('effective_until')

            try:
                limit_upper = threshold_set['limits']['upper']
            except KeyError:
                limit_upper = None

            try:
                limit_lower = threshold_set['limits']['lower']
            except KeyError:
                limit_lower = None

            try:
                warning_upper = threshold_set['warnings']['upper']
            except KeyError:
                warning_upper = None

            try:
                warning_lower = threshold_set['warnings']['lower']
            except KeyError:
                warning_lower = None

            if field.python_type is int:
                result.append(ProductFieldConfiguration(
                    int(limit_lower) if limit_lower is not None else None,
                    int(limit_upper) if limit_upper is not None else None,
                    int(warning_lower) if warning_lower is not None else None,
                    int(warning_upper) if warning_upper is not None else None,
                    effective_since, effective_until))
            elif field.python_type is float:
                result.append(ProductFieldConfiguration(
                    float(limit_lower) if limit_lower is not None else None,
                    float(limit_upper) if limit_upper is not None else None,
                    float(warning_lower) if warning_lower is not None else None,
                    float(warning_upper) if warning_upper is not None else None,
                    effective_since, effective_until))
            elif field.python_type is datetime.datetime:
                logging.warning(
                    f'Parsing of datetime values from quality-checks file is not yet implemented - returning null constraints')
            elif field.python_type is datetime.date:
                logging.warning(
                    f'Parsing of date values from quality-checks file is not yet implemented - returning null constraints')
            else:
                raise ValueError(
                    f'python type "{field.python_type.__name__}" not in supported python types (int, float, datetime, date)')

        return result

    def describe(self):
        description = {
            'effective_since': self.effective_since,
            'effective_until': self.effective_until,
            'warnings': {
                'lower': self.low_warning_value,
                'upper': self.high_warning_value
            },
            'limits': {
                'lower': self.min_valid_value,
                'upper': self.max_valid_value
            }

        }

        return description
