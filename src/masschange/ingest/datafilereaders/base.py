from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime
from typing import Dict, Any, Union, Type, Callable, Optional

import numpy as np
import pandas as pd

from masschange.datasets.timeseriesdatasetfield import TimeSeriesDatasetField


class DataFileReader(ABC):

    @classmethod
    @abstractmethod
    def get_input_file_default_regex(cls) -> str:
        """Return the regex pattern to identify relevant datafiles by filename"""
        pass

    @classmethod
    @abstractmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        """Return the regex pattern to identify relevant compressed files containing datafiles, by filename"""
        pass

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
    def extract_stream_id(cls, filepath: str) -> str:
        """Given a path to a data file, return the id of the stream (usually satellite) to which the file relates"""
        pass

    @classmethod
    @abstractmethod
    def get_fields(cls) -> Collection[TimeSeriesDatasetField]:
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
        # It is currently assumed that rcvtime_intg and rcvtime_frac are common across most datasets.
        # If this is not the case, refactoring will be necessary.
        raw_data = cls._load_raw_data_from_file(filepath)

        try:
            constant_columns = [column for column in cls.get_input_column_defs() if column.is_constant]
            for column in constant_columns:
                cls._ensure_constant_column_value(column.name, column.const_value, raw_data)
        except ValueError as err:
            raise ValueError(f'Const-valued column check failed for {filepath}: {err}')

        # TODO: investigate whether dropping/excluding const columns prior to pd df construction improves performance
        #  at all
        df = pd.DataFrame(raw_data)

        df['timestamp'] = df.apply(cls.populate_timestamp, axis=1)

        # Drop extraneous columns
        df = df.drop([col.name for col in cls.get_input_column_defs() if col.is_constant], axis=1)

        return df

    @classmethod
    @abstractmethod
    def populate_timestamp(cls, row) -> datetime:
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
            usecols=([col.index for col in column_defs]),
            dtype=[(col.name, col.np_type) for col in column_defs]
        )

        return data

    @classmethod
    def _ensure_constant_column_value(cls, column_name: str, expected_value: Any, data: np.ndarray):
        """Ensure that a constant-valued column only contains the expected value, raising ValueError on failure"""
        column_data = data[column_name]
        cls._ensure_constant_array_value(column_name,  expected_value, column_data)

    @classmethod
    def _ensure_constant_array_value(cls,  column_name: str,  expected_value: Any, column_data: np.ndarray):
        """Ensure that an array only contains the expected value, raising ValueError on failure"""
        unexpected_data = np.where(column_data != expected_value)
        if unexpected_data[0].size != 0:
            first_bad = column_data[unexpected_data[0][0]]

            raise ValueError(f'Unexpected value for const-valued field "{column_name} "'
                             f'expected: "{expected_value}", was: "{first_bad}"')

    @classmethod
    def extract_stream_id(cls, filepath: str) -> str:
        filename = os.path.split(filepath)[-1]
        satellite_id_char = re.search(cls.get_input_file_default_regex(), filename).group('stream_id')
        return satellite_id_char

    @classmethod
    def get_fields(cls) -> Collection[TimeSeriesDatasetField]:
        return cls.get_input_column_defs()

class DataFileWithProdFlagReader(AsciiDataFileReader):

    @classmethod
    def _get_prod_flag_column_position(cls) -> int:

        """Return index(0-based) of 'prod_flag' column in an input ASCII file"""
        for column in cls.get_input_column_defs():
            if column.name == 'prod_flag':
                return column.index
        raise ValueError('Can not find "prod_file" column')

    @classmethod
    def _get_first_prod_flag_data_column_position(cls) -> int:
        """
        Return index(0-based) of first column in the input ASCII file with data defined by prod_flag.
        It is not always the next column after the prod_flag column
        TODO: This assumes that prod_data is contiguous and always at the end of the row
        """
        for column in cls.get_input_column_defs():
            if isinstance(column, ProdFlagInputColumn):
                return column.index
        raise ValueError('Can not find any ProdFlagInputColumn column in the input file')

    @classmethod
    def load_data_from_file(cls, filepath: str) -> pd.DataFrame:

        # get raw data as 2D array of strings
        raw_data_as_str = cls._load_raw_data_from_file(filepath)

        # prepare data for variables defined in the prod_flag, with np.nan for missing data
        prod_flag_data_expanded = cls._get_expanded_prod_flag_data(raw_data_as_str)

        # create an empty data frame
        df = pd.DataFrame()

        # add regular (not prod_flag) columns to DF column-by-column, converting to corresponding data type
        reg_columns = [col for col in cls.get_input_column_defs() if not
                                        isinstance(col, ProdFlagInputColumn)]
        for column in reg_columns:
            values = np.array(raw_data_as_str[:,column.index]).astype(column.np_type)
            # Check const columns, do not add const columns to the DF
            if column.is_constant:
                try:
                    cls._ensure_constant_array_value(column.name, column.const_value, values)
                except ValueError as err:
                     raise ValueError(f'Const-valued column check failed for {filepath}: {err}')
            else:
                df[column.name] = values
        df['timestamp'] = df.apply(cls.populate_timestamp, axis=1)

        # append prod_flag columns at the end
        prod_flag_col = cls.get_prod_flag_output_column_defs()

        for i, col in enumerate(prod_flag_col):
            #TODO: np.nan is defined for float data only.Might throw if we see
            # missing prod_flag data of different type
            df[col.name] = np.array(prod_flag_data_expanded[:,i]).astype(col.np_type)

        return df

    @classmethod
    def _get_prod_flag_for_defined_columns(cls, orig_prod_flag: np.array)->np.array:
        """
        Get prod_flag as 2D numpy array of integer, drop columns of bits that are not defined
        orig_prod_flag: prod_flag data from the input file as 1D np.array of strings
        """
        # convert original prod_flag into 2D array of int
        prod_flag = np.array(list(map(list, orig_prod_flag))).astype(int)
        # flip left to right, to correspond the order of ProdFlagOutputColumn
        prod_flag = np.fliplr(prod_flag)

        # drop columns for which  ProdFlagOutputColumn is not defined
        bit_idx = [col.prod_flag_bit_index for col in cls.get_prod_flag_output_column_defs()]
        return prod_flag[:, bit_idx]

    @classmethod
    def _get_expanded_prod_flag_data(cls, raw_data):
        prod_flag_orig = raw_data[:, cls._get_prod_flag_column_position()]
        prod_flag = cls._get_prod_flag_for_defined_columns(prod_flag_orig)

        start_of_prod_flag_data = cls._get_first_prod_flag_data_column_position()
        prod_flag_data = raw_data[:,start_of_prod_flag_data:]
        prod_flag_expanded_data = np.full(prod_flag.shape, np.nan, dtype=object)
        prod_flag_expanded_data[np.where(prod_flag == 1)] = prod_flag_data.flatten()
        return prod_flag_expanded_data

    @classmethod
    def _load_raw_data_from_file(cls, filename: str) -> np.ndarray:
        header_line_count = cls.get_header_line_count(filename)
        # get data as arrays of strings, because data types for input
        # columns are not known in advance
        df = pd.read_csv(filename, skiprows=header_line_count, header=None, sep=" +", dtype = str, engine='python')
        return df.values

    @classmethod
    def get_fields(cls) -> Collection[TimeSeriesDatasetField]:
        reg_columns = [col for col in cls.get_input_column_defs() if
                       (not isinstance(col, ProdFlagInputColumn) and (not col.is_constant))]
        prod_flag_columns = [col for col in cls.get_prod_flag_output_column_defs()]
        return reg_columns + prod_flag_columns

    @classmethod
    @abstractmethod
    def get_prod_flag_output_column_defs(cls) -> Collection[ProdFlagOutputColumn]:
        """
        Get definitions for all columns defined in the prod_flag for a product
        """
        pass

class AsciiDataFileReaderColumn(TimeSeriesDatasetField):
    """
    Defines an individual column to extract from a tabular ASCII data file, including any transforms to be applied

    Attributes
        index (int): the tabular index of the field in the input file

        name (str): the field name, (and the name to give the numpy column for the extracted data)

        type (Type | str): the type, numpy dtype, or numpy dtype string representing the column type to extract with numpy

        transform (Callable[[T], T]): a transform (or wrapper for series of transforms) to apply to the extracted values, if applicable

        const_value(Any | None): an optional assumed_constant value for the column, which is validated during ingestion
    """

    index: int
    np_type: Union[Type, str]
    transform: Callable[[Any], Any]

    def __init__(self, index: int, name: str, np_type: Union[Type, str], aggregations: Collection[str] = None,
                 transform: Union[Callable[[Any], Any], None] = None, const_value: Optional[Any] = None):
        super().__init__(name, aggregations=aggregations, const_value=const_value)
        self.index = index
        self.name = name
        self.np_type = np_type
        self.transform = transform or self._no_op

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

class ProdFlagInputColumn(AsciiDataFileReaderColumn):
    """
    Defines an individual column to extract from a tabular ASCII data file which holds different
    data based on prod_flag bit
    """
    def __init__(self, index: int, name: str, np_type: Union[Type, str]):
        super().__init__(index, name, np_type = np_type, aggregations = None, transform = self._no_op, const_value = None)

class ProdFlagOutputColumn(AsciiDataFileReaderColumn):
    """
    Defines an individual column created by reader that holds data for an individual variable
    defined in prod_flag. Some values in this column could be np.nan

    Attributes
        prod_flag_bit_index (int): the index of the bit for this variable in the prod_flag,  right to left, 0-based
    """
    prod_flag_bit_index: int
    def __init__(self, index: int, prod_flag_bit_index: int,
                 name: str, np_type: Union[Type, str],  aggregations: Collection[str] = None,
                 transform: Union[Callable[[Any], Any], None] = None, const_value: Optional[Any] = None ):
        super().__init__(index, name, np_type, aggregations = aggregations,
                 transform = transform, const_value = const_value)
        self.prod_flag_bit_index = prod_flag_bit_index

