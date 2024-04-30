from abc import abstractmethod
from collections.abc import Set, Collection
from datetime import datetime
from typing import Union, Any, Dict, Type


class TimeSeriesDatasetField:
    """
    An abstract class for encapsulating field information common to both reader and presentation/API.

    Attributes
        name (str): the field's name, as used in the database and presented via the API
        const_value (Any | None): an optional value for the column, which is assumed to be constant across every datum '
        of a given data product, which is validated during ingestion
        VALID_AGGREGATIONS (StrEnum): a set of enumerated aggregations which may

    """

    name: str
    description: str
    unit: str
    const_value: Union[Any, None]
    aggregations: Set[str] = set()

    VALID_AGGREGATIONS: Set[str] = {'MIN', 'MAX', 'AVG'}

    def __init__(self, name: str, unit: str, description: str = "", aggregations: Collection[str] = None,
                 const_value: Union[Any, None] = None):
        self.name = name.lower()
        self.unit = unit
        self.description = description
        self.const_value = const_value

        if aggregations is not None:
            if not all([agg in self.VALID_AGGREGATIONS for agg in aggregations]):
                raise ValueError(f'aggregations arg must be subset of {self.VALID_AGGREGATIONS} (got {aggregations})')
            self.aggregations = set(aggregations)

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
        return {f'{self.name}_{agg.lower()}' for agg in self.aggregations}

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
            'supported_aggregations': sorted([agg.lower() for agg in self.aggregations]),
        }

        if self.is_constant:
            description['constant_value'] = self.const_value,

        return description


class TimeSeriesDatasetTimestampField(TimeSeriesDatasetField):
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

