from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime
from typing import Union, Any, Dict, Type, Set

from masschange.ingest.executor.utils.aggregations import Aggregation, TrivialAggregation


class TimeSeriesDataProductField(ABC):
    """
    An abstract class for encapsulating field information common to both reader and presentation/API.

    Attributes
        name (str): the field's name, as used in the database and presented via the API
        description(str): a description which may be displayed in the presentation layer (API)
        unit (str | None): the unit of measurement for the field's values
        const_value (Any | None): an optional value for the column, which is assumed to be constant across every datum '
        of a given data product, which is validated during ingestion
        aggregations (StrEnum): a set of enumerated aggregations which are valid when data is downsampled.
        is_lookup_field (bool): True if this field resolved at query-time from a source other than the dataset table,
         i.e. location, which is resolved from the GNV data using the timestamp
        is_time_series_id_column (bool): True if this field contains an identifier which differentiates distinct
         time-series (ex. sensor id)

    """

    name: str
    description: str
    unit: Union[str, None]
    const_value: Union[Any, None]
    aggregations: Set[Aggregation]
    is_lookup_field: bool = False  # only True via subclass override
    is_time_series_id_column = False

    VALID_BASIC_AGGREGATIONS: Set[str] = {'min', 'max', 'avg'}

    def __init__(self, name: str, unit: Union[str, None], description: str = "",
                 aggregations: Collection[Union[str, Aggregation]] = None,
                 const_value: Union[Any, None] = None, is_lookup_field: bool = False, is_time_series_id_column: bool = False):
        self.name = name.lower()
        self.unit = unit
        self.description = description
        self.const_value = const_value
        self.is_lookup_field = is_lookup_field
        self.is_time_series_id_column = is_time_series_id_column
        self.aggregations = set()

        if aggregations is not None:
            string_typed_aggregations = [agg for agg in aggregations if isinstance(agg, str)]
            if not all([agg in self.VALID_BASIC_AGGREGATIONS for agg in string_typed_aggregations]):
                raise ValueError(
                    f'str-typed elements of aggregations arg must be subset of {self.VALID_BASIC_AGGREGATIONS} (got {aggregations}, including str-typed elements {string_typed_aggregations})')

            for agg in aggregations:
                if isinstance(agg, Aggregation):
                    self.aggregations.add(agg)
                elif isinstance(agg, str):
                    # included for simple/legacy declaration of aggregations
                    self.aggregations.add(TrivialAggregation(agg))
                else:
                    raise ValueError(
                        f'elements of aggregations arg must be either type str or Aggregation (got {agg})')

    @property
    @abstractmethod
    def python_type(self) -> Type:
        """Return the python type of the data provided by this field"""
        raise NotImplementedError(f'python_type has not been implemented for {self.__class__} with name {self.name}')

    @property
    def is_aggregable(self) -> bool:
        """Return whether statistical aggregation is supported for this field"""
        supported_types = {int, float}
        return self.python_type in supported_types and not self.is_constant
    @property
    def is_constant(self):
        return self.const_value is not None

    @property
    def has_aggregations(self):
        return len(self.aggregations) > 0

    def __hash__(self):
        return self.name.__hash__()

    def __eq__(self, other):
        return self.name == other.name

    def describe(self) -> Dict:
        description = {
            'name': self.name,
            'type': self.python_type.__name__,
            'description': self.description,
            'unit': self.unit,
            'supported_aggregations': sorted([agg.describe(self.name) for agg in self.aggregations],
                                             key=lambda agg_dict: agg_dict.get('type')),
            'is_time_series_id': self.is_time_series_id_column
        }

        if self.is_constant:
            description['constant_value'] = self.const_value,

        return description


class TimeSeriesDataProductTimestampField(TimeSeriesDataProductField):
    def __init__(self, name: str, unit: Union[str, None], description: str = "", aggregations: Collection[str] = None,
                 const_value: Union[Any, None] = None):

        if aggregations is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None aggregations arg')

        if const_value is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None const_value arg')

        super().__init__(name, unit, description)

    @property
    def python_type(self) -> Type:
        return datetime


class TimeSeriesDataProductLocationLookupField(TimeSeriesDataProductField):
    """Location field which is dynamically resolved at query-time, loading values from the GNV data tables."""

    def __init__(self, name: str, unit: Union[str, None], description: str = "", aggregations: Collection[str] = None,
                 const_value: Union[Any, None] = None):

        if aggregations is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None aggregations arg')

        if const_value is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None const_value arg')

        super().__init__(name, unit, description, is_lookup_field=True)

    @property
    def python_type(self) -> Type:
        return dict
