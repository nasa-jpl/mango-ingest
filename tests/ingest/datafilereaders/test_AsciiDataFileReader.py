import logging
import os
import unittest
from datetime import datetime
from typing import Sequence, Type, Tuple, Dict, List

import numpy as np

from masschange.ingest import ingest
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection
from masschange.ingest.datafilereaders.base import AsciiDataFileReader
from masschange.ingest.datafilereaders.gracefoacc1a import GraceFOAcc1ADataFileReader

log = logging.getLogger()


class AsciiDataFileReaderTestCase(unittest.TestCase):
    def test_check_const_fields(self):
        class CheckConstFieldsStubReader(AsciiDataFileReader):
            @classmethod
            def get_reference_epoch(cls) -> datetime:
                return datetime(2000, 1, 1, 12)

            @classmethod
            def get_input_file_default_regex(cls) -> str:
                raise NotImplementedError

            @classmethod
            def get_zipped_input_file_default_regex(cls) -> str:
                raise NotImplementedError

            @classmethod
            def get_input_column_defs(cls):
                float_value_dtype = np.double
                return [
                    {'index': 0, 'label': 'variable_int_col', 'type': np.ulonglong},
                    {'index': 1, 'label': 'const_int_col', 'type': np.uint},
                    {'index': 2, 'label': 'const_char_col', 'type': 'U1'},
                    {'index': 3, 'label': 'const_str_col', 'type': 'U4'},
                    {'index': 4, 'label': 'const_float_col', 'type': float_value_dtype},
                    {'index': 5, 'label': 'const_scifloat_col', 'type': float_value_dtype},
                ]

            @classmethod
            def get_const_column_expected_values(cls):
                return {
                    'const_int_col': 1,
                    'const_char_col': 'A',
                    'const_str_col': 'ABCD',
                    'const_float_col': 1.234,
                    'const_scifloat_col': 23.45
                }

            @classmethod
            def get_timestamp_input_column_labels(cls) -> List:
                return []

            @classmethod
            def populate_rcvtime(cls, row) -> int:
                pass

            @classmethod
            def populate_timestamp(cls, row) -> datetime:
                pass

        reader = CheckConstFieldsStubReader()
        filepath = './tests/input_data/ingest/datafilereaders/test_AsciiDataFileReader_test_check_const_fields_failure.txt'
        raw_data = reader._load_raw_data_from_file(filepath)

        for column_label, expected_value in reader.get_const_column_expected_values().items():
            with self.assertRaises(ValueError):
                reader._ensure_constant_column_value(column_label, expected_value, raw_data)
