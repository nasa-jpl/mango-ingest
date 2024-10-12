import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.hrt1a_pass import GraceFOHrt1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOHrt1PassDatasetDatasetIngestTestCaseBase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOHrt1APassDataProduct
    expected_table_names = ['gracefo_hrt1a_pass_04_c', 'gracefo_hrt1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('HRT1A_2018-06-01_C_NYA_581169412.pass', 581083200, 607671580, 581169412.505, 581180868.505,
         717, 16, 0.0001057017416800032, 15.99800002574921, 16.00199997425079,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
          datetime(2018, 6, 1, 23, 56, 52, 505000, tzinfo=timezone.utc)),
        ('HRT1A_2018-06-01_D_NYA_581163783.pass', 581083200, 607671633, 581163783.505, 581175159.505,
         712, 16, 0.0001500096278294242, 15.99800002574921, 16.00199997425079,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 22, 23, 3, 505000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
