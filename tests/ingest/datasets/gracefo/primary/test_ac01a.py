import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.ac01a import GraceFOAc01ADataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAc01ADatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAc01ADataProduct
    expected_table_names = ['gracefo_ac01a_04_c', 'gracefo_ac01a_04_d']
    expected_field_types = [int, int, str, str, float, float, float, float, float, float, int,
                            bool, bool, bool, bool, bool, bool, bool, bool, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (741430700,  27781,  'C',  '00000000',  0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 9964,
         False, False, False, False, False, False, False, False,
         datetime(2023, 6, 30, 20, 58, 20, 27781, tzinfo=timezone.utc)),
        (741430700, 1701, 'D', '00000000', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 9964,
         False, False, False, False, False, False, False, False,
         datetime(2023, 6, 30, 20, 58, 20, 1701, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
