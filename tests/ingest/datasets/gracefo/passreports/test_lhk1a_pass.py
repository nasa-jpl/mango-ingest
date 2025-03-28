import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.lhk1a_pass import GraceFOLhk1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLhk1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOLhk1APassDataProduct
    expected_table_names = ['gracefo_lhk1a_pass_04_c', 'gracefo_lhk1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('LHK1A_2018-06-11_C_NYA_581975176.pass', 581947200, 607671334, 581975176.0333574, 581975299.0106808,
         88, 1.413532453021784, 13.10856070032623, 0, 122.9773234128952,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 11, 0, 0, tzinfo=timezone.utc)),
        ('LHK1A_2018-06-12_D_NYA_582060335.pass', 582033600, 607671431, 582060335.0372616, 582060457.0131339,
         88, 1.402021520439236, 13.00181270302831, 0, 121.9758722782135,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 12, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
