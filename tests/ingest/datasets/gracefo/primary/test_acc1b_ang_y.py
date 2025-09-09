import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.acc1b_ang_y import GraceFOAcc1bAngYDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAcc1bAngYDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAcc1bAngYDataProduct
    expected_table_names = ['gracefo_acc1b_ang_y_00_c', 'gracefo_acc1b_ang_y_00_d']
    expected_field_types = [int, float, datetime]
    expected_table_row_counts = [101,101]
    expected_table_first_rows = [
        (738849371, -0.017661124241163506,
         datetime(2023, 5, 31, 23, 56, 11, tzinfo=timezone.utc)),
        (738849348, -2.941517688458987,
         datetime(2023, 5, 31, 23, 55, 48, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
