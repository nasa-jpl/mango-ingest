import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.tim1a_pass import GraceFOTim1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOTim1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOTim1APassDataProduct
    expected_table_names = ['gracefo_tim1a_pass_04_c', 'gracefo_tim1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('TIM1A_2018-05-22_C_NYA_580300954.pass', 580219200, 607673386, 580300954.0,
            580312282.0, 1417, 8.0, 0.0,
            8.0, 8.0, 8, 0, 0, 0, 1417, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        ('TIM1A_2018-05-22_D_NYA_580282629.pass', 580219200, 607673509, 580282629.0,
            580306589.0, 2996, 8.0, 0.0,
            8.0, 8.0, 8, 0, 0, 0, 2996, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
