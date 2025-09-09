import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.acc1b_lin_x import GraceFOAcc1bLinXDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAcc1bLinXDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAcc1bLinXDataProduct
    expected_table_names = ['gracefo_acc1b_lin_x_00_c', 'gracefo_acc1b_lin_x_00_d']
    expected_field_types = [int, float, datetime]
    expected_table_row_counts = [101,101]
    expected_table_first_rows = [
        (738849371, 63.802372945138956,
         datetime(2023, 5, 31, 23, 56, 11, tzinfo=timezone.utc)),
        (738849348, 81.44849951996048,
         datetime(2023, 5, 31, 23, 55, 48, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
