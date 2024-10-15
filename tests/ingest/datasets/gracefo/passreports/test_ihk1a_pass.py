import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.ihk1a_pass import GraceFOIhk1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOIhk1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOIhk1APassDataProduct
    expected_table_names = ['gracefo_ihk1a_pass_04_c', 'gracefo_ihk1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('IHK1A_2018-06-01_C_NYA_581169239.pass', 581083200, 607670129, 581169239, 581180639,
         3724, 3.06204673650282, 13.20404006255819, 0, 60,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
          datetime(2018, 6, 1, 23, 53, 59, tzinfo=timezone.utc)),
        ('IHK1A_2018-06-01_D_NYA_581163589.pass', 581083200, 607670151, 581163589, 581174929,
         3705, 3.061555075593953, 13.20303696331802, 0, 60,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 22, 19, 49, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
