import os
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Sequence, Dict, Any, List

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
    def get_input_column_defs(cls) -> Sequence[Dict]:
        """
        Return a sequence of columns to extract from the ASCII CSV data file, in the following format:
        {'index': $columnIndex, 'label' $columnName, 'type': $numpyType}
        """
        pass

    @classmethod
    @abstractmethod
    def get_const_column_expected_values(cls) -> Dict[str, Any]:
        """
        Some fields are expected to have one single value for every row in an entire data product.  Return a mapping of
        every const-valued column label to its expected value.
        """
        pass

    @classmethod
    @abstractmethod
    def get_reference_epoch(cls) -> datetime:
        """Return the reference epoch used as the basis of rcvtime fields"""
        pass

    @classmethod
    def get_header_line_count(cls, filename: str) -> int:
        last_header_line_prefix = '# End of YAML header'

        header_rows = 0
        with open(filename) as f:
            for line in f:  # iterates lazily
                header_rows += 1
                if line.startswith(last_header_line_prefix):
                    return header_rows

    @classmethod
    def load_data_from_file(cls, filepath: str) -> pd.DataFrame:
        # It is currently assumed that rcvtime_intg and rcvtime_frac are common across most datasets.
        # If this is not the case, refactoring will be necessary.
        raw_data = cls._load_raw_data_from_file(filepath)

        try:
            for column_label, expected_value in cls.get_const_column_expected_values().items():
                cls._ensure_constant_column_value(column_label, expected_value, raw_data)
        except ValueError as err:
            raise ValueError(f'Const-valued column check failed for {filepath}: {err}')

        # TODO: investigate whether dropping/excluding const columns prior to pd df construction improves performance
        #  at all
        df = pd.DataFrame(raw_data)

        df['timestamp'] = df.apply(cls.populate_timestamp, axis=1)

        # Drop extraneous columns
        df = df.drop(list(cls.get_const_column_expected_values().keys()), axis=1)

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
            usecols=([col['index'] for col in column_defs]),
            dtype=[(col['label'], col['type']) for col in column_defs]
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
