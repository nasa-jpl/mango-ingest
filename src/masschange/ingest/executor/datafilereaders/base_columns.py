from __future__ import annotations

from typing import Callable, Any, Union, Type, Collection, Optional
import numpy as np

from masschange.dataproducts.dataproductfield import DataProductField
from masschange.db.data.aggregations import Aggregation


class AsciiDataFileReaderColumn(DataProductField):
    """
    Defines an individual column to extract from a tabular ASCII data file, including any transforms to be applied

    Attributes
        index (int): the tabular index of the field in the input file

        name (str): the field name, (and the name to give the numpy column for the extracted data)

        np_dtype (np.dtype): The numpy dtype to which extracted data will be cast.
         Constructed from a Python type, numpy dtype, or numpy array-protocol type string.

         See https://numpy.org/doc/stable/reference/arrays.dtypes.html ctrl+f "array-protocol type string" for further
         details on the string aliases used by numpy.

        description(str): a description which may be displayed in the presentation layer (API)

        aggregations (StrEnum): a set of enumerated aggregations which are valid when data is downsampled

        transform (Callable[[T], T]): a transform (or wrapper for series of transforms) to apply to the extracted values, if applicable

        const_value(Any | None): an optional assumed_constant value for the column, which is validated during ingestion

        is_channel_id_column (bool): True if this field contains an identifier which differentiates distinct

    """

    index: int
    np_dtype: np.dtype
    transform: Callable[[Any], Any]

    def __init__(self, index: int, name: str, np_type: Union[Type, str], unit: Union[str, None], description: str = "",
                 aggregations: Collection[Union[str, Aggregation]] = None, transform: Union[Callable[[Any], Any], None] = None,
                 const_value: Optional[Any] = None, is_channel_id_column: bool = False):
        super().__init__(name, unit, description=description, aggregations=aggregations, const_value=const_value,
                         is_channel_id_column=is_channel_id_column)
        self.index = index
        self.np_dtype = np.dtype(np_type)
        self.transform = transform or self._no_op

    @property
    def python_type(self):
        try:
            # default case, where self.np_dtype is a native numpy dtype
            resolved_type = type(self.np_dtype.type(0).item())
        except AttributeError:
            # edge case, where self.np_dtype is a pandas type (like nullable integer type Int64Dtype)
            resolved_type = type(self.np_dtype.type(0))

        return resolved_type

    @property
    def has_transform(self):
        """Return whether the column has a transform defined"""
        return self.transform is not self._no_op

    @staticmethod
    def _no_op(x):
        return x

    @property
    def is_constant(self):
        return self.const_value is not None


class VariableSchemaAsciiDataFileReaderColumn(AsciiDataFileReaderColumn):
    """
    Defines an individual column created by reader that holds data for an individual variable
    defined in prod_flag. Some values in this column could be np.nan

    Attributes
        prod_flag_bit_index (int): the index of the bit for this variable in the prod_flag, right to left, 0-based
    """
    prod_flag_bit_index: int

    def __init__(self, prod_flag_bit_index: int, name: str, np_type: Union[Type, str], unit: Union[str, None], description='',
                 aggregations: Collection[str] = None, transform: Union[Callable[[Any], Any], None] = None,
                 const_value: Optional[Any] = None, is_channel_id_column: bool = False):
        super().__init__(None, name, np_type, unit, description=description, aggregations=aggregations, transform=transform,
                         const_value=const_value, is_channel_id_column=is_channel_id_column)
        self.prod_flag_bit_index = prod_flag_bit_index


class DerivedAsciiDataFileReaderColumn(AsciiDataFileReaderColumn):
    """
    Defines an individual column created by reader that holds data
    that are not read directly from a particular column in the product file,
    but derived from data in the product file, possibly from different columns.
    """

    def __init__(self, name: str, np_type: Union[Type, str], unit: Union[str, None], description='', aggregations: Collection[Union[str, Aggregation]] = None,
                 transform: Union[Callable[[Any], Any], None] = None, const_value: Optional[Any] = None, is_channel_id_column: bool = False):
        if const_value is not None:
            raise ValueError(f'it is not valid to instantiate a DerivedAsciiDataFileReaderColumn with a const value')

        super().__init__(None, name, np_type, unit, description=description, aggregations=aggregations, transform=transform,
                         const_value=None, is_channel_id_column=is_channel_id_column)


class ArrayLikeAsciiDataFileReaderColumn(AsciiDataFileReaderColumn):
    """
    Defines a column created by reader that holds array-like data (for example, quality flags).
    During the ingestion, we want to parse such data and append each flag to the database table
    as a separate boolean column
    """
    array_size: int  # Number of flags that array-lake data column holds

    def __init__(self, index: int, name: str, np_type: Union[Type, str], array_size: int, description: str = ""):
        super().__init__(index, name, np_type, unit=None, description=description,
                         aggregations=None, transform=None, const_value=None, is_channel_id_column=False)

        self.array_size = array_size
