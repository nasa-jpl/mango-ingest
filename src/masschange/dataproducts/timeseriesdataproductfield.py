from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime
from typing import Union, Any, Dict, Type, Set

from masschange.ingest.utils.aggregations import Aggregation, TrivialAggregation


class TimeSeriesDataProductField(ABC):
    """
    An abstract class for encapsulating field information common to both reader and presentation/API.

    Attributes
        name (str): the field's name, as used in the database and presented via the API
        description(str): a description which may be displayed in the presentation layer (API)
        unit (str): the unit of measurement for the field's values
        const_value (Any | None): an optional value for the column, which is assumed to be constant across every datum '
        of a given data product, which is validated during ingestion
        aggregations (StrEnum): a set of enumerated aggregations which are valid when data is downsampled.
        is_derived (bool): True if this field is derived from (but not stored as a column in) the database,
         i.e. location, which is derived from timestamp and the GNV data

    """

    name: str
    description: str
    unit: str
    const_value: Union[Any, None]
    aggregations: Set[Aggregation]
    is_derived: bool  = False  # only True via subclass override

    VALID_BASIC_AGGREGATIONS: Set[str] = {'MIN', 'MAX', 'AVG'}

    def __init__(self, name: str, unit: str, description: str = "", aggregations: Collection[Union[str, Aggregation]] = None,
                 const_value: Union[Any, None] = None, is_derived: bool = False):
        self.name = name.lower()
        self.unit = unit
        self.description = description
        self.const_value = const_value
        self.is_derived = is_derived
        self.aggregations = set()

        if aggregations is not None:
            string_typed_aggregations = [agg for agg in aggregations if isinstance(agg, str)]
            if not all([agg in self.VALID_BASIC_AGGREGATIONS for agg in string_typed_aggregations]):
                raise ValueError(f'str-typed elements of aggregations arg must be subset of {self.VALID_BASIC_AGGREGATIONS} (got {aggregations}, including str-typed elements {string_typed_aggregations})')

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
    def is_constant(self):
        return self.const_value is not None

    @property
    def has_aggregations(self):
        return len(self.aggregations) > 0

    @property
    def aggregation_db_column_names(self) -> Set[str]:
        # TODO: Excise calls to this function
        return {agg.get_aggregated_name(self.name) for agg in self.aggregations}

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
            'supported_aggregations': sorted([agg.get_aggregated_name(self.name) for agg in self.aggregations]),
        }

        if self.is_constant:
            description['constant_value'] = self.const_value,

        return description


class TimeSeriesDataProductTimestampField(TimeSeriesDataProductField):
    def __init__(self, name: str, unit: str, description: str = "", aggregations: Collection[str] = None,
                 const_value: Union[Any, None] = None):

        if aggregations is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None aggregations arg')

        if const_value is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None const_value arg')

        super().__init__(name, unit, description)

    @property
    def python_type(self) -> Type:
        return datetime


class GraceFOGnv1ALocationField(TimeSeriesDataProductField):
    @property
    def python_type(self) -> Type:
        return dict


class TimeSeriesDataProductDerivedLocationField(TimeSeriesDataProductField):
    def __init__(self, name: str, unit: str, description: str = "", aggregations: Collection[str] = None,
                 const_value: Union[Any, None] = None):

        if aggregations is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None aggregations arg')

        if const_value is not None:
            raise ValueError(f'{self.__class__} cannot be instantiated with non-None const_value arg')

        super().__init__(name, unit, description, is_derived=True)

    @property
    def python_type(self) -> Type:
        return dict
