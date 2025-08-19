import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.ac01b import GraceFOAc01BDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAc01BDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAc01BDataProduct
    expected_table_names = ['gracefo_ac01b_04_c', 'gracefo_ac01b_04_d']
    expected_field_types = [int, str, float, float, float, float, float, float, float, float, float, str,
                            bool, bool, bool, bool, bool, bool, bool, bool, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (741441600, 'C', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, '00000000',
         False, False, False, False, False, False, False, False,
         datetime(2023, 7, 1, 0, 0, 0, tzinfo=timezone.utc)),
        (741441600, 'D', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  '00000000',
         False, False, False, False, False, False, False, False,
         datetime(2023, 7, 1, 0, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
