import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.plt1a_rpt import GraceFOPlt1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOPlt1BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOPlt1ARptDataProduct
    expected_table_names = ['gracefo_plt1a_rpt_04_y']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('PLT1A_2023-06-01_Y_04.dat', 738849600, 739546918, 738849301, 738936300,
         174000, 0.4999971264202668, 0.4999999999917425, 0, 1,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
    ]
if __name__ == '__main__':
    unittest.main()
