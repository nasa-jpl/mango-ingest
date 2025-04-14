from collections.abc import Sequence

from masschange.ingest.executor.datafilereaders.base import DerivedAsciiDataFileReaderColumn
from masschange.dataproducts.dataproductfield import DataProductField

def append_flag_fields(df, source_col_name):
    """
    Parse array-like fields into boolean elements and add columns to the dataframe.
    The name of the columns is <source_col_name>_<index>

    Parameters
    ----------
    df: pd.DataFrame - Data frame to append boolean columns. Mast have a column with name specified in source_col_name
    source_col_name:str - Name of the array-like columns with

    """
    if source_col_name not in df.columns:
        raise ValueError(
            f'Can not append flag field columns to the dataframe. Source column "{source_col_name}" does not exist')
    num_elements = len(df[source_col_name][0])
    for i in range(num_elements):
        col_name = f'{source_col_name}_{str(i)}'
        df[col_name] = df.apply(populate_flag, idx=i, axis=1, result_type='expand')


def populate_flag(row, idx) -> str:
    return row.qualflg[idx]


def generate_array_of_fields(base_field_name: str, array_size: int) -> Sequence[DerivedAsciiDataFileReaderColumn]:
    """
    Given a name of a field and array_size, returns list of objects of type 'DerivedAsciiDataFileReaderColumn'
    with names composed as <base_field_name>_<str(n)>, where n is range of int from 0 to array_size-1
    """
    return [DerivedAsciiDataFileReaderColumn(f'{base_field_name}_{str(i)}', np_type=bool, unit=None)
            for i in range(array_size)]

