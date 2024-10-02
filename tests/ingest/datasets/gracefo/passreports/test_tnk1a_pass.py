import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.tnk1a_pass import GraceFOTnk1APassDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOTnk1PassDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOTnk1APassDataProduct
    expected_table_names = ['gracefo_tnk1a_pass_04_c', 'gracefo_tnk1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('TNK1A_2018-06-01_C_NYA_581169412.pass', 581083200, 607671585, 581169412.381, 581180868.505,
         7164, 1.599347200886956, 1.934767346251998, 0, 4.000999927520752,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 23, 56, 52, 381000, tzinfo=timezone.utc)),
        ('TNK1A_2018-06-01_D_NYA_581163770.pass', 581083200, 607671638, 581163770.381, 581175166.381,
         7124, 1.599887687772006, 1.787080439289095, 0, 4.002999901771545,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 22, 22, 50, 381000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
