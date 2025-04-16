from collections.abc import Sequence
from pandas import DataFrame as pdDataFrame, Series as pdSeries

from masschange.ingest.executor.datafilereaders.base_columns import DerivedAsciiDataFileReaderColumn


def append_flag_fields(df: pdDataFrame, source_col_name: str) -> None:
    """
    Parse array-like fields into int ot boolean columns and append to the dataframe.
    The name of the columns are <source_col_name>_<index>

    Parameters
    ----------
    df: DataFrame - Frame to append int or boolean columns. Mast have an array-like column with name specified in source_col_name
    source_col_name:str - Name of the array-like column
    -------
    """
    if source_col_name not in df.columns:
        raise ValueError(
            f'Can not append flag field columns to the dataframe. Source column "{source_col_name}" does not exist')
    num_elements = len(df[source_col_name][0])
    for i in range(num_elements):
        col_name = f'{source_col_name}_{str(i)}'
        df[col_name] = df.apply(populate_flag, idx=i, source_col_name=source_col_name, axis=1, result_type='expand')


def populate_flag(row: pdSeries, source_col_name: str, idx: int) -> int:
    """
    Given a row from Panda's frame, extract character at position 'idx'
    from array-like column specified by 'source_col_name'

    Parameters
    ----------
    row: row from Panda's dataframe. A row in a Pandas DataFrame is a Pandas Series object
    source_col_name: name of the array-like column in Panda's frame
    idx: index of element in array-like column to extract
    Returns  value at the inx position of the array-like element, as int
    -------

    """
    return int(row[source_col_name][idx])


def generate_array_of_fields(base_field_name: str, array_size: int) -> Sequence[DerivedAsciiDataFileReaderColumn]:
    """
    Given a name of a field and array_size, returns list of objects of type 'DerivedAsciiDataFileReaderColumn'
    with names composed as <base_field_name>_<str(n)>, where n is range of int from 0 to array_size-1

    Parameters
    ----------
    base_field_name: name of array-like column that will be used as a prefix for derived column names
    array_size: number of elements in the array-like data. For qualflag column, it is a length of the string

    Returns List of DerivedAsciiDataFileReaderColumn that represent the new boolean columns derived from array-like column
    -------

    """
    return [DerivedAsciiDataFileReaderColumn(f'{base_field_name}_{str(i)}', np_type=bool, unit=None)
            for i in range(array_size)]

