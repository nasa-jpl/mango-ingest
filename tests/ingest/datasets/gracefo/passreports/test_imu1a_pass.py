import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.imu1a_pass import GraceFOImu1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOImu1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOImu1APassDataProduct
    expected_table_names = ['gracefo_imu1a_pass_04_c', 'gracefo_imu1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('IMU1A_2018-06-01_C_NYA_581169228.pass', 581083200, 607670128, 581169228.003935, 581180687.879144,
         366720, 0.03124974492451682, 0.2901112000813258, -0.8707519769668579, 0.1285649538040161,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
          datetime(2018, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('IMU1A_2018-06-01_D_NYA_581163589.pass', 581083200, 607670150, 581163589.003945, 581174988.879156,
         364800, 0.03124974358756587, 0.2901110547474888, -0.8707529306411743, 0.1285700798034668,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
