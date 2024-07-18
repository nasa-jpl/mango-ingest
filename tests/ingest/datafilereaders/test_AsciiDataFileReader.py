import logging
import unittest
from datetime import datetime
from typing import List

import numpy as np

from masschange.ingest.datafilereaders.base import AsciiDataFileReader, AsciiDataFileReaderColumn

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
                    AsciiDataFileReaderColumn(index=0, name='variable_int_col', np_type=np.ulonglong,
                                              unit='implement_me'),
                    AsciiDataFileReaderColumn(index=1, name='const_int_col', np_type=np.uint, unit='implement_me',
                                              const_value=1),
                    AsciiDataFileReaderColumn(index=2, name='const_char_col', np_type='U1', unit='implement_me',
                                              const_value='A'),
                    AsciiDataFileReaderColumn(index=3, name='const_str_col', np_type='U4', unit='implement_me',
                                              const_value='ABCD'),
                    AsciiDataFileReaderColumn(index=4, name='const_float_col', np_type=float_value_dtype,
                                              unit='implement_me', const_value=1.234),
                    AsciiDataFileReaderColumn(index=5, name='const_scifloat_col', np_type=float_value_dtype,
                                              unit='implement_me', const_value=23.45),
                ]

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
                reader._ensure_constant_column_value(column.name, column.const_value, raw_data)
