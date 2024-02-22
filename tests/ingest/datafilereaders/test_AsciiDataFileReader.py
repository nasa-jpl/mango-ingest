import logging
import os
import unittest
from datetime import datetime
from typing import Sequence, Type, Tuple, Dict, List

import numpy as np

from masschange.ingest import ingest
from masschange.datasets.timeseriesdataset import TimeSeriesDataset
from masschange.db import get_db_connection
from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn
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
                legacy_column_defs = [
                    {'index': 0, 'label': 'variable_int_col', 'type': np.ulonglong},
                    {'index': 1, 'label': 'const_int_col', 'type': np.uint, 'const_value': 1},
                    {'index': 2, 'label': 'const_char_col', 'type': 'U1', 'const_value': 'A'},
                    {'index': 3, 'label': 'const_str_col', 'type': 'U4', 'const_value': 'ABCD'},
                    {'index': 4, 'label': 'const_float_col', 'type': float_value_dtype, 'const_value': 1.234},
                    {'index': 5, 'label': 'const_scifloat_col', 'type': float_value_dtype, 'const_value': 23.45},
                ]

                return [AsciiDataFileReaderColumn.from_legacy_definition(col) for col in legacy_column_defs]

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

        const_columns = [col for col in reader.get_input_column_defs() if col.is_constant]
        for column in const_columns:
            with self.assertRaises(ValueError):
                reader._ensure_constant_column_value(column.label, column.const_value, raw_data)
