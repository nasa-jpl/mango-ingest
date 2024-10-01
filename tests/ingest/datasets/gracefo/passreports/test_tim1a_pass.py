import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.tim1a_pass import GraceFOTim1APassDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOTim1PassDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOTim1APassDataProduct
    expected_table_names = ['gracefo_tim1a_pass_04_c', 'gracefo_tim1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('TIM1A_2018-06-01_C_NYA_581169413.pass', 581083200, 607671582, 581169413,
         581180861, 1432, 8, 0, 8, 8,
         8, 0, 0, 1432, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 23, 56, 53, tzinfo=timezone.utc)),
        ('TIM1A_2018-06-01_D_NYA_581163776.pass', 581083200, 607671635, 581163776, 581175168,
         1425, 8, 0, 8, 8,
         8, 0, 0, 1425, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 22, 22, 56, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
