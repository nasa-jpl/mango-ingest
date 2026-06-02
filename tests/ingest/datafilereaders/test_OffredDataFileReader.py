import logging
import unittest
from datetime import datetime
import os

import numpy as np
import pandas as pd
from masschange.ingest.executor.datafilereaders.base_offred import OffredFileReader
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn

log = logging.getLogger()

class StubGraceFOOffredDataFileReader(GraceFOOffredDataFileReader):

    @classmethod
    def _get_current_input_file_column_def(cls, data_fpath: str):
        return super()._get_current_input_file_column_def(data_fpath, check_time_col_names = False)

class OffredFileReaderTestCase(unittest.TestCase):
    data_file = './tests/input_data/offred/GF1_CX_XXX_4_221520919_0001.zip'
    @classmethod
    def setUpClass(cls)-> None:
        os.environ['OFFRED_METADATA_FILEPATH'] = './tests/input_data/offred/fake_fields_metadata.json'

    @classmethod
    def tearDownClass(cls):
        os.environ.pop('OFFRED_METADATA_FILEPATH', None)

    # def test_get_field_names(self):
    #
    #     field_names = GraceFOOffredDataFileReader._get_field_names(self.data_file)
    #     self.assertEqual(field_names[1], 'XXX2')
    #     self.assertEqual(field_names[-2], 'EEE.en')

    def test_load_raw_data_from_file(self):
        data = StubGraceFOOffredDataFileReader._load_raw_data_from_file(self.data_file)
        self.assertEqual(data['utc'][24], 'value1')
        self.assertEqual(data['value_str'][24], '')
        self.assertEqual(data['value_int'][24], None)
        self.assertEqual(data['unit'][24], '')
        self.assertEqual(data['pcf_name'][24], 'FFF.ev')
        self.assertEqual(data['value_int'][30],'7')
        self.assertEqual(data['unit'][30], 'eee_unit')
        self.assertEqual(data['pcf_name'][30],'EEE.en')

    # def test_get_data_column_types(self):
    #     types = GraceFOOffredDataFileReader._get_data_column_types(self.data_file, 100)
    #
    #     self.assertEqual(types[0], pd.Int64Dtype)
    #     self.assertEqual(types[1], np.float32)
    #     self.assertEqual(types[2], np.float32)
    #     self.assertEqual(types[3], np.float32)
    #     self.assertEqual(types[4], 'U100')
    #     self.assertEqual(types[5], pd.Int64Dtype)
    #     self.assertEqual(types[6], 'U100')
