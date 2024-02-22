from typing import Union, Any


class TimeSeriesDatasetField:
    """
    An abstract class for encapsulating field information common to both reader and presentation/API.

    Attributes
        name (str): the field's name, as used in the database and presented via the API
        const_value (Any | None): an optional value for the column, which is assumed to be constant across every datum '
        of a given data product, which is validated during ingestion

    """

    name: str
    const_value: Union[Any, None]

    def __init__(self, name: str, const_value: Union[Any, None] = None):
        self.name = name
        self.const_value = const_value

    @property
    def is_constant(self):
        return self.const_value is not None
