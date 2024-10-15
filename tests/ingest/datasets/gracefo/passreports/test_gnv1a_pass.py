import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.gnv1a_pass import GraceFOGnv1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOGnv1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOGnv1APassDataProduct
    expected_table_names = ['gracefo_gnv1a_pass_04_c', 'gracefo_gnv1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('GNV1A_2018-06-01_C_NYA_581169228.pass', 581083200, 607670129, 581169228, 581180686,
         5730, 2, 0, 2, 2, 8, 0, 0, 0, 0, 0, 0, 0, 0,
          datetime(2018, 6, 1, 23, 53, 48, tzinfo=timezone.utc)),
        ('GNV1A_2018-06-01_D_NYA_581163588.pass', 581083200, 607670151, 581163588, 581174986, 5700,
         2, 0, 2, 2, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 22, 19, 48, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
