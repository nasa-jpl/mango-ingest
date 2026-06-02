import unittest
import os
from datetime import datetime, timezone
from typing import Union
from masschange.dataproducts.implementations.gracefo.offred.offred import GraceFOOffredDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.ingest.executor.datafilereaders.base_columns import AsciiDataFileReaderColumn


class StubGraceFOOffredDataFileReader(GraceFOOffredDataFileReader):

    @classmethod
    def _get_current_input_file_column_def(cls, data_fpath: str):
        return super()._get_current_input_file_column_def(data_fpath, check_time_col_names = False)

class GraceFOOffredDatasetReaderTestCase(DatasetReaderTestCaseBase):
    class StubGraceFOOffredDataProduct(GraceFOOffredDataProduct):
        @classmethod
        def get_reader(cls):
            return StubGraceFOOffredDataFileReader()

    @classmethod
    def setUpClass(cls) -> None:
        os.environ['OFFRED_METADATA_FILEPATH'] = './tests/input_data/offred/fake_fields_metadata.json'
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        os.environ.pop('OFFRED_METADATA_FILEPATH', None)
        super().tearDownClass()

    test_data_path = './tests/input_data/offred/'
    #data_is_zipped = False
    data_is_zipped = True
    maxDiff = None
    dataset_cls = StubGraceFOOffredDataProduct
    expected_table_names = ['gracefo_offred_00_gf1', 'gracefo_offred_00_gf2']
    expected_field_types = [str, str, str, Union[str, None], Union[int, None], Union[float, None],
                            Union[str, None], datetime]
    expected_table_row_counts = [42, 42]
    expected_table_first_rows = [
        ('YYY',  '777777XXX7777777777',
         'GGG.ff', None, None, None, 'TTTTT', datetime(2022, 4, 7, 2, 22, 12, 555000, tzinfo=timezone.utc)),
        ('SSS', '777777XXX7777777777',
         'DDD.en', 'ddd_unit', None, None, 'ZZZZ',
         datetime(2022, 4, 7, 2, 22, 13, 777000, tzinfo=timezone.utc))

    ]

if __name__ == '__main__':
    unittest.main()
