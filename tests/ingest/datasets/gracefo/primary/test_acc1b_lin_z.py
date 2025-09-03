import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.acc1b_lin_z import GraceFOAcc1bLinZDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAcc1bLinZDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAcc1bLinZDataProduct
    expected_table_names = ['gracefo_acc1b_lin_z_04_c', 'gracefo_acc1b_lin_z_04_d']
    expected_field_types = [int, float, datetime]
    expected_table_row_counts = [101,101]
    expected_table_first_rows = [
        (738849371, 0.6330908587163151,
         datetime(2023, 5, 31, 23, 56, 11, tzinfo=timezone.utc)),
        (738849348, -50.5229992999075,
         datetime(2023, 5, 31, 23, 55, 48, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
