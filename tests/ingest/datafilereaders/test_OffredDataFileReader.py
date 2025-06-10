import logging
import unittest
from datetime import datetime
from typing import List

import numpy as np

from masschange.ingest.executor.datafilereaders.base_offred import OffredFileReader
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred_num import GraceFOOffredNumDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn

log = logging.getLogger()


class OffredCheckClassMethodsFieldsStubReader(OffredFileReader):
    @classmethod
    def get_reference_epoch(cls) -> datetime:
        return datetime(2000, 1, 1, 12)

    @classmethod
    def get_input_file_default_regex(cls) -> str:
        pass

    @classmethod
    def get_zipped_input_file_default_regex(cls) -> str:
        pass

    @classmethod
    def get_input_column_defs(cls):
        pass

    @classmethod
    def populate_rcvtime(cls, row) -> int:
        pass

    @classmethod
    def populate_timestamp(cls, row) -> datetime:
        pass

class OffredFileReaderTestCase(unittest.TestCase):

    def test_get_row_for_offred_field_name(self):
        offred_excel_filepath = './tests/input_data/offred/SRDB_G1.4.3e_PCFdat.xlsx'
        field_name = 'AHT10012'
        field_metadata = \
            OffredCheckClassMethodsFieldsStubReader._get_row_for_offred_field_name(offred_excel_filepath, field_name)
        self.assertEqual(field_metadata['NAME'], field_name)
        self.assertEqual(field_metadata['UNIT'], 'DegC')
        self.assertEqual(field_metadata['CATEG'], 'N')

    def test_get_field_names(self):
        filepath = './tests/input_data/offred/GF1_CX_21127B_NSG_4_221520919_0001_22152062031_22152084031.out'

        field_names = OffredCheckClassMethodsFieldsStubReader._get_field_names(filepath)
        self.assertEqual(field_names[1], 'OBT_Integer')
        self.assertEqual(field_names[-2], 'TTT52790.rn')

    def test_load_raw_data_from_file(self):
        filepath = './tests/input_data/offred/GF1_CX_21127B_NSG_4_221520919_0001_22152062031_22152084031.out'

        data = GraceFOOffredNumDataFileReader._load_raw_data_from_file(filepath)
        self.assertEqual(data['utc'][0], '2022.152.06.20.31.886')
        self.assertEqual(data['field_value'][-1], 31249784)
