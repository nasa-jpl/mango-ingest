from __future__ import annotations

import abc
import os
import re
from abc import ABC, abstractmethod
from collections.abc import Collection
from datetime import datetime
from typing import Dict, Any, Union, Type, Callable, Optional

import numpy as np
import pandas as pd


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
                cls._ensure_constant_column_value(column.label, column.const_value, raw_data)
        except ValueError as err:
            raise ValueError(f'Const-valued column check failed for {filepath}: {err}')

        # TODO: investigate whether dropping/excluding const columns prior to pd df construction improves performance
        #  at all
        df = pd.DataFrame(raw_data)

        df['timestamp'] = df.apply(cls.populate_timestamp, axis=1)

        # Drop extraneous columns
        df = df.drop([col.label for col in cls.get_input_column_defs() if col.is_constant], axis=1)

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
            dtype=[(col.label, col.np_type) for col in column_defs]
        )

        return data

    @classmethod
    def _ensure_constant_column_value(cls, column_label: str, expected_value: Any, data: np.ndarray):
        """Ensure that a constant-valued column only contains the expected value, raising ValueError on failure"""
        column_data = data[column_label]
        unexpected_data = np.where(column_data != expected_value)
        if unexpected_data[0].size != 0:
            first_bad = column_data[unexpected_data[0][0]]
            raise ValueError(f'Unexpected value for const-valued field "{column_label} "'
                             f'expected: "{expected_value}", was: "{first_bad}"')

    @classmethod
    def extract_stream_id(cls, filepath: str) -> str:
        filename = os.path.split(filepath)[-1]
        satellite_id_char = re.search(cls.get_input_file_default_regex(), filename).group('stream_id')
        return satellite_id_char


class DataFileReaderField(abc.ABC):
    """
    An abstract class for inheriting implementation-agnostic behaviour common to all reader implementations.
    Currently, this is just the concept of a field having an assumed-constant value, though this may expand in future

    Attributes
        const_value (Any | None): an optional value for the column, which is assumed to be constant across every datum '
        of a given data product, which is validated during ingestion

    """

    const_value: Union[Any, None]

    def __init__(self, const_value: Union[Any, None]):
        self.const_value = const_value

    @property
    def is_constant(self):
        return self.const_value is not None


class AsciiDataFileReaderColumn(DataFileReaderField):
    """
    Defines an individual column to extract from a tabular ASCII data file, including any transforms to be applied

    Attributes
        index (int): the tabular index of the field in the input file

        label (str): the label to give the extracted

        type (Type | str): the type, numpy dtype, or numpy dtype string representing the column type to extract with numpy

        transform (Callable[[T], T]): a transform (or wrapper for series of transforms) to apply to the extracted values, if applicable

        const_value(Any | None): an optional assumed_constant value for the column, which is validated during ingestion
    """

    index: int
    label: str
    np_type: Union[Type, str]
    transform: Callable[[Any], Any]

    def __init__(self, index: int, label: str, np_type: Union[Type, str],
                 transform: Union[Callable[[Any], Any], None] = None, const_value: Optional[Any] = None):
        super().__init__(const_value)
        self.index = index
        self.label = label
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

    @classmethod
    def from_legacy_definition(cls, legacy_definition: Dict[str, Any]) -> AsciiDataFileReaderColumn:
        """Create an AsciiDataFileReaderColumn from the old-style dict-based definition, with null transform"""
        return AsciiDataFileReaderColumn(
            index=legacy_definition['index'],
            label=legacy_definition['label'],
            np_type=legacy_definition['type'],
            const_value=legacy_definition.get('const_value')
        )
