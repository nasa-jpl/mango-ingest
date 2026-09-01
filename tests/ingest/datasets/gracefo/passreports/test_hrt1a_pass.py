import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.hrt1a_pass import GraceFOHrt1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOHrt1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOHrt1APassDataProduct
    expected_table_names = ['gracefo_hrt1a_pass_04_c', 'gracefo_hrt1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('HRT1A_2018-05-22_C_NYA_580300961.pass', 580219200, 607673384, 580300961.506,
            580312289.506, 709, 16.0, 0.0,
            16.0, 16.0, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        ('HRT1A_2018-05-22_D_NYA_580282628.pass', 580219200, 607673505, 580282628.506,
            580306580.506, 1498, 16.0, 0.0001266211422447545,
            15.99799990653992, 16.00200009346008, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
