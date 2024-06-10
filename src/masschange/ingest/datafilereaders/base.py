from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime, timedelta
from typing import Dict, Any, Union, Type, Callable, Optional

import numpy as np
import pandas as pd

from masschange.ingest.errors import EmptyProductException
from masschange.dataproducts.timeseriesdataproductfield import TimeSeriesDataProductField
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion


class DataFileReader(ABC):

    @classmethod
    @abstractmethod
    def get_input_file_default_regex(cls) -> str:
        """
        Return the regex pattern to identify relevant datafiles by filename.
        Must implement the following named capture groups:
            - stream_id
            - dataset_version
        """
        pass

    @classmethod
    @abstractmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        """
        Return the regex pattern to identify relevant compressed files containing datafiles, by filename
        Must implement the following named capture groups:
            - stream_id
            - dataset_version
        """
        pass

    @classmethod
    def extract_stream_id(cls, filepath: str) -> str:
        """Extract stream id from unzipped input file"""
        filename = os.path.split(filepath)[-1]
        satellite_id_char = re.search(cls.get_input_file_default_regex(), filename).group('stream_id')
        return satellite_id_char

    @classmethod
    def extract_dataset_version(cls, filepath: str) -> TimeSeriesDatasetVersion:
        """Extract version id from unzipped input file"""
        filename = os.path.split(filepath)[-1]
        dataset_version_id = re.search(cls.get_input_file_default_regex(), filename).group('dataset_version')
        return TimeSeriesDatasetVersion(dataset_version_id)

    @classmethod
    @abstractmethod
    def load_data_from_file(cls, filepath: str) -> pd.DataFrame:
        """Given a path to a source file, return a pandas dataframe containing fully-prepared/transformed data, ready
        for insertion to the database."""
        # TODO: if rcvtime/timestamp columns are consistent across data products, it may be appropriate to provide a
        #  default implementation here
        pass

    @classmethod
    @abstractmethod
    def _load_raw_data_from_file(cls, filepath: str) -> np.ndarray:
        """Given a path to a source file, extract data from the desired columns as a numpy ndarray"""
        pass

    @classmethod
    @abstractmethod
    def get_fields(cls) -> Collection[TimeSeriesDataProductField]:
        """Return implementation-agnostic definitions for the fields ingested by the reader"""
        pass


class AsciiDataFileReader(DataFileReader):

    @classmethod
    @abstractmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        """
        Return a collection of columns to extract from the ASCII CSV data file
        """
        pass

    @classmethod
    @abstractmethod
    def get_reference_epoch(cls) -> datetime:
        """Return the reference epoch used as the basis of rcvtime fields"""
        pass

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        last_header_line_prefixes = ['# End of YAML header', 'END OF HEADER']

        header_rows = 0
        with open(filename) as f:
            for line in f:  # iterates lazily
                header_rows += 1
                for hdr_end_prefix in last_header_line_prefixes:
                    if line.startswith(hdr_end_prefix):
                        return header_rows
        raise ValueError(f'Can not find the end of header in {filename}')

    @classmethod
    def load_data_from_file(cls, filepath: str) -> pd.DataFrame:
        # It is currently assumed that rcvtime_intg and rcvtime_frac are common across most dataproducts.
        # If this is not the case, refactoring will be necessary.
        raw_data = cls._load_raw_data_from_file(filepath)
        if raw_data.size == 0:
            raise EmptyProductException(f'{filepath} seems to have no data...')

        try:
            constant_columns = [column for column in cls.get_input_column_defs() if column.is_constant]
            for column in constant_columns:
                cls._ensure_constant_column_value(column.name, column.const_value, raw_data)
        except ValueError as err:
            raise ValueError(f'Const-valued column check failed for {filepath}: {err}')

        # TODO: investigate whether dropping/excluding const columns prior to pd df construction improves performance
        #  at all
        df = pd.DataFrame(raw_data)

        # Append custom fields to the dataframe, if needed
        cls.append_derived_fields(df)

        df['timestamp'] = df.apply(cls.populate_timestamp, axis=1)

        # Drop extraneous columns
        df = df.drop([col.name for col in cls.get_input_column_defs() if col.is_constant], axis=1)

        return df

    @classmethod
    @abstractmethod
    def populate_timestamp(cls, row) -> datetime:
        pass

    @classmethod
    def append_derived_fields(cls, df):
        """
        Use this method to append fields that are not read directly from the product file,
        for example, geolocation field

        Parameters
        ----------
        df -  pd.DataFrame

        """
        pass

    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:
        header_line_count = cls.get_header_line_count(filename)
        # TODO: extract indices, descriptions, units dynamically from the header?
        # TODO: use prodflag and/or QC for filtering measurements?

        column_defs = cls.get_input_column_defs()
        data = np.loadtxt(
            fname=filename,
            skiprows=header_line_count,
            delimiter=None,  # split rows by whitespace chunks
            usecols=([col.index for col in column_defs if col.index is not None ]),
            dtype=[(col.name, col.np_dtype) for col in column_defs if col.index is not None],
            ndmin = 1 # set to 1 to prevent returning a single row as a list instead of array
        )

        return data

    @classmethod
    def _ensure_constant_column_value(cls, column_name: str, expected_value: Any, data: np.ndarray):
        """Ensure that a constant-valued column only contains the expected value, raising ValueError on failure"""
        column_data = data[column_name]
        cls._ensure_constant_array_value(column_name, expected_value, column_data)

    @classmethod
    def _ensure_constant_array_value(cls, column_name: str, expected_value: Any, column_data: np.ndarray):
        """Ensure that an array only contains the expected value, raising ValueError on failure"""
        unexpected_data = np.where(column_data != expected_value)
        if unexpected_data[0].size != 0:
            first_bad = column_data[unexpected_data[0][0]]

            raise ValueError(f'Unexpected value for const-valued field "{column_name} "'
                             f'expected: "{expected_value}", was: "{first_bad}"')

    @classmethod
    def get_fields(cls) -> Collection[TimeSeriesDataProductField]:
        return cls.get_input_column_defs()


class DataFileWithProdFlagReader(AsciiDataFileReader):

    @classmethod
    def load_data_from_file(cls, filepath: str) -> pd.DataFrame:

        # get raw data as 2D array of strings
        raw_data_as_str = cls._load_raw_data_from_file(filepath)
        if raw_data_as_str.size == 0:
            raise EmptyProductException(f'{filepath} seems to have no data...')

        # create an empty data frame
        df = pd.DataFrame()

        # add regular (not prod_flag) columns to DF column-by-column, converting to corresponding data type
        reg_columns = [col for col in cls.get_input_column_defs() if not
        isinstance(col, VariableSchemaAsciiDataFileReaderColumn)]
        for column in reg_columns:
            values = np.array(raw_data_as_str[:, column.index]).astype(column.np_dtype)
            # Check const columns, do not add const columns to the DF
            if column.is_constant:
                try:
                    cls._ensure_constant_array_value(column.name, column.const_value, values)
                except ValueError as err:
                    raise ValueError(f'Const-valued column check failed for {filepath}: {err}')
            else:
                df[column.name] = values
        # add timestamp
        df['timestamp'] = df.apply(cls.populate_timestamp, axis=1)

        # append variable schema data at the end of the frame
        cls.append_variable_schema_data(raw_data_as_str, df)

        return df

    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:
        header_line_count = cls.get_header_line_count(filename)
        # get data as arrays of strings, because data types for input
        # columns are not known in advance

        #  Provide read_csv with array of dummy column names,
        #  so the number of columns in the output data frame would be equal to the
        #  length of the name list
        dummy_column_names = [i for i in range(len(cls.get_input_column_defs()))]
        df = pd.read_csv(filename, skiprows=header_line_count, header=None, sep=" +", dtype=str, engine='python',
                         names=dummy_column_names)
        return df.values

    @classmethod
    def append_variable_schema_data(cls, raw_data_as_str: np.array, df: pd.DataFrame) -> pd.DataFrame:
        # prepare data for variables defined in the prod_flag, with np.nan for missing data
        prod_flag_data_expanded = cls._get_expanded_prod_flag_data(raw_data_as_str)

        # append prod_flag columns at the end
        prod_flag_col = [col for col in cls.get_input_column_defs() \
                         if isinstance(col, VariableSchemaAsciiDataFileReaderColumn)]

        for i, col in enumerate(prod_flag_col):
            # Use pd.array here to handle nullable int datatype pd.Int64Dtype
            df[col.name] = pd.array(prod_flag_data_expanded[:, i], dtype=col.np_dtype)

    @classmethod
    def _get_prod_flag_column_position(cls) -> int:

        """Return index(0-based) of 'prod_flag' column in an input ASCII file"""
        for column in cls.get_input_column_defs():
            if column.name == 'prod_flag':
                return column.index
        raise ValueError('Can not find "prod_file" column')

    @classmethod
    @abstractmethod
    def _get_first_prod_flag_data_column_position(cls) -> int:
        """
        Return index(0-based) of first column in the input ASCII file with data defined by prod_flag.
        It is not always the next column after the prod_flag column
        TODO: This assumes that prod_data is contiguous and always at the end of the row
        """
        pass

    @classmethod
    def _get_prod_flag_for_defined_columns(cls, orig_prod_flag: np.array) -> np.array:
        """
        Get prod_flag as 2D numpy array of integer, drop columns of bits that are not defined

        orig_prod_flag: prod_flag data from the input file as 1D np.array of strings
        """
        # convert original prod_flag into 2D array of int
        prod_flag = np.array(list(map(list, orig_prod_flag))).astype(int)
        # flip left to right, to correspond the order of VariableSchemaAsciiDataFileReaderColumn
        prod_flag = np.fliplr(prod_flag)

        # drop columns for which  VariableSchemaAsciiDataFileReaderColumn is not defined
        bit_idx = [col.prod_flag_bit_index for col in cls.get_input_column_defs() \
                   if isinstance(col, VariableSchemaAsciiDataFileReaderColumn)]
        return prod_flag[:, bit_idx]

    @classmethod
    def _get_expanded_prod_flag_data(cls, raw_data):
        prod_flag_orig = raw_data[:, cls._get_prod_flag_column_position()]
        prod_flag = cls._get_prod_flag_for_defined_columns(prod_flag_orig)

        start_of_prod_flag_data = cls._get_first_prod_flag_data_column_position()
        prod_flag_data = raw_data[:, start_of_prod_flag_data:]

        # drop all None from prod_flag_data
        prod_flag_data = prod_flag_data[np.not_equal(prod_flag_data, None)]

        # init 2D np array of objects with np.nan
        prod_flag_expanded_data = np.full(prod_flag.shape, np.nan, dtype=object)

        # populate array with data from input file according to prod_flag

        prod_flag_expanded_data[np.where(prod_flag == 1)] = prod_flag_data
        return prod_flag_expanded_data

class ReportFileReader(AsciiDataFileReader):
    """
    Base reader for report files
    """

    @classmethod
    def get_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        """
        All report files have 19 standard colunms, which are defined here.
        Some report files have additional columns, which will be defined
        in the get_rpt_custom_input_column_defs() method of a child classes.
        """

        # Note: name of the columns are from the GMAT tool,
        # except instead of 'camel case' we use low case separated by underscore.

        standard_columns = [
            AsciiDataFileReaderColumn(index=0, name='file_name', np_type='U32', unit=None),
            AsciiDataFileReaderColumn(index=1, name='file_tag', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=2, name='process_ttag', np_type=np.ulonglong, unit='s'),
            AsciiDataFileReaderColumn(index=3, name='first_data_point_t_tag', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=4, name='last_data_point_t_tag',  np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=5, name='n_recs', np_type=int, unit=None),
            AsciiDataFileReaderColumn(index=6, name='time_gap_avg', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=7, name='time_gap_var', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=8, name='time_gap_min', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=9, name='time_gap_max', np_type=np.double, unit='s'),
            AsciiDataFileReaderColumn(index=10, name='n_qual_bits', np_type=np.ubyte,  unit=None),
            AsciiDataFileReaderColumn(index=11, name='bit_count_0', np_type=np.byte, unit=None),
            AsciiDataFileReaderColumn(index=12, name='bit_count_1', np_type=np.byte, unit=None),
            AsciiDataFileReaderColumn(index=13, name='bit_count_2', np_type=np.byte, unit=None),
            AsciiDataFileReaderColumn(index=14, name='bit_count_3', np_type=np.byte, unit=None),
            AsciiDataFileReaderColumn(index=15, name='bit_count_4', np_type=np.byte, unit=None),
            AsciiDataFileReaderColumn(index=16, name='bit_count_5', np_type=np.byte, unit=None),
            AsciiDataFileReaderColumn(index=17, name='bit_count_6', np_type=np.byte, unit=None),
            AsciiDataFileReaderColumn(index=18, name='bit_count_7', np_type=np.byte, unit=None)
            ]
        return standard_columns + cls.get_rpt_custom_input_column_defs()

    @classmethod
    def get_rpt_custom_input_column_defs(cls) -> Collection[AsciiDataFileReaderColumn]:
        """
        Return definition of additional columns (if any) specific for a particular product type.
        Overwrite this method in a child class if a data type has additional report columns'''
        """
        return []

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        return cls.get_reference_epoch() + timedelta(seconds=row.file_tag)

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        return 0


class VariableDataClustersPerRowReader(AsciiDataFileReader):
    """
    This reader works with data clusters that are repeated
    variable number of times per line in the product file.
    It re-formats data to a single cluster per row format
    """

    @classmethod
    def _get_max_num_of_clusters_per_row(cls, filename):
        header_line_count = cls.get_header_line_count(filename)

        # Read clusters-per-row counter from the data file to calculate max number of columns
        counter_col_name = cls._get_clusters_counter_col_name()
        column_defs = cls.get_input_column_defs()
        data = np.loadtxt(
            fname=filename,
            skiprows=header_line_count,
            delimiter=None,  # split rows by whitespace chunks
            usecols=([col.index for col in column_defs if col.name == counter_col_name]),
            dtype=[(col.name, col.np_dtype) for col in column_defs if col.name == counter_col_name]
        )
        return int(np.max(data[counter_col_name]))

    @classmethod
    def _columns_idx_to_drop(cls, n_cols):
        column_defs = cls.get_input_column_defs()
        idx_to_keep = [col.index for col in column_defs if not isinstance(col, DerivedAsciiDataFileReaderColumn)]
        clusters_start_pos = cls._get_first_cluster_column_position()
        return  [i for i in range(n_cols) if i not in idx_to_keep and i < clusters_start_pos]

    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:

        clusters_start_pos = cls._get_first_cluster_column_position()
        cluster_size = cls._get_num_variables_in_cluster()

        # calculate number of columns we need to read the data
        n_cols = clusters_start_pos + cls._get_max_num_of_clusters_per_row(filename) * cluster_size

        # read all data to a data frame
        dummy_column_names = [i for i in range(n_cols)]
        df = pd.read_csv(filename, skiprows=cls.get_header_line_count(filename),
                         header=None, sep=" +", dtype=str, engine='python', names=dummy_column_names)

        # drop columns that we don't need
        df = df.drop(df.columns[cls._columns_idx_to_drop(n_cols)], axis=1)

        # calculate number of rows in the reformatted frame, where we will have one cluster per row
        column_defs = cls.get_input_column_defs()
        counter_idx = [col.index for col in column_defs if \
                       col.name == cls._get_clusters_counter_col_name()][0]
        # TODO: check if idx is found

        n_reformat_rows = np.sum(df[counter_idx].astype('int'))
        n_reformat_col = len(column_defs)
        # create 2D array to hold reformatted data
        reformat_array = np.full((n_reformat_rows, n_reformat_col), np.nan, dtype=object)

        # create 1D array to hold data for a row in the reformatted array
        reformat_row = np.full(n_reformat_col, np.nan, dtype=object)

        reformat_array_idx = 0

        # iterate over each row in the raw data array and populate the reformatted array
        df = df.reset_index()  # make sure indexes pair with number of rows
        num_prefix_col = len([col.index for col in column_defs if not isinstance(col, DerivedAsciiDataFileReaderColumn)])
        for index, row in df.iterrows():
            n_clusters_in_row = int(row[counter_idx])
            row = row[1:]
            for i in range(n_clusters_in_row):
                reformat_row[0:num_prefix_col] = row.values[:num_prefix_col]
                reformat_row[num_prefix_col:] = \
                    row.values[num_prefix_col + i*cluster_size: num_prefix_col + (i+1) * cluster_size]

                reformat_array[reformat_array_idx] = reformat_row
                reformat_array_idx = reformat_array_idx + 1

        # convert to structured array, so we can use load_data_from_file from the parent class
        return np.core.records.fromarrays(reformat_array.transpose(),
                                          dtype=np.dtype([(col.name, col.np_dtype) for col in column_defs]))

    @classmethod
    @abstractmethod
    def _get_first_cluster_column_position(cls) -> int:
        """
        Return index(0-based) of the start of repeated clusters in the input ASCII file
        """
        pass

    @classmethod
    @abstractmethod
    def _get_clusters_counter_col_name(cls) -> int:
        """
        Return name of a column that specifies number of repeated clusters per row
        """
        pass

    @classmethod
    @abstractmethod
    def _get_num_variables_in_cluster(cls) -> int:
        """
        Return number of variables in data cluster
        """
        pass


class AsciiDataFileReaderColumn(TimeSeriesDataProductField):
    """
    Defines an individual column to extract from a tabular ASCII data file, including any transforms to be applied

    Attributes
        index (int): the tabular index of the field in the input file

        name (str): the field name, (and the name to give the numpy column for the extracted data)

        np_dtype (np.dtype): The numpy dtype to which extracted data will be cast.
         Constructed from a Python type, numpy dtype, or numpy array-protocol type string.

         See https://numpy.org/doc/stable/reference/arrays.dtypes.html ctrl+f "array-protocol type string" for further
         details on the string aliases used by numpy.

        transform (Callable[[T], T]): a transform (or wrapper for series of transforms) to apply to the extracted values, if applicable

        const_value(Any | None): an optional assumed_constant value for the column, which is validated during ingestion
    """

    index: int
    np_dtype: np.dtype
    transform: Callable[[Any], Any]

    def __init__(self, index: int, name: str, np_type: Union[Type, str], unit: str, description: str = "",
                 aggregations: Collection[str] = None, transform: Union[Callable[[Any], Any], None] = None,
                 const_value: Optional[Any] = None):
        super().__init__(name, unit, description=description, aggregations=aggregations, const_value=const_value)
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

    def __init__(self, prod_flag_bit_index: int, name: str, np_type: Union[Type, str], unit, description='',
                 aggregations: Collection[str] = None, transform: Union[Callable[[Any], Any], None] = None,
                 const_value: Optional[Any] = None):
        super().__init__(None, name, np_type, unit, description=description, aggregations=aggregations, transform=transform,
                         const_value=const_value)
        self.prod_flag_bit_index = prod_flag_bit_index


class DerivedAsciiDataFileReaderColumn(AsciiDataFileReaderColumn):
    """
    Defines an individual column created by reader that holds data
    that are not read directly from a particular column in the product file,
    but derived from data in the product file, possibly from different columns.
    """

    def __init__(self, name: str, np_type: Union[Type, str], unit, description='', aggregations: Collection[str] = None,
                 transform: Union[Callable[[Any], Any], None] = None, const_value: Optional[Any] = None):
        if const_value is not None:
            raise ValueError(f'it is not valid to instantiate a DerivedAsciiDataFileReaderColumn with a const value')

        super().__init__(None, name, np_type, unit, description=description, aggregations=aggregations, transform=transform,
                         const_value=None)



