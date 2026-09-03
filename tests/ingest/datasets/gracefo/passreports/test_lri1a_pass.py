import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.lri1a_pass import GraceFOLri1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLri1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOLri1APassDataProduct
    expected_table_names = ['gracefo_lri1a_pass_04_c', 'gracefo_lri1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('LRI1A_2018-06-14_C_NYA_582254632.pass', 582206400, 607671656, 582254632.907648,
            582258945.7180785, 41680, 0.1034768211935683, 3.762358776614844e-08,
            0.1034767627716064, 0.103476881980896, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 6, 14, 0, 0, tzinfo=timezone.utc)),
        ('LRI1A_2018-06-14_D_NYA_582254630.pass', 582206400, 607671666, 582254630.4951711,
            582263320.1970998, 83980, 0.1034747011602279, 7.518951927102881e-08,
            0.1034746170043945, 0.1034747362136841, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 6, 14, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
