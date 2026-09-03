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
        ('IMU1A_2018-05-22_C_NEN_580291418.pass', 580219200, 607670781, 580291418.00394,
            580301008.879138, 306136, 0.03132890782826737, 0.2899949215726549,
            -0.870730996131897, 0.1281640529632568, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        ('IMU1A_2018-05-22_D_NEN_580291418.pass', 580219200, 607670752, 580291418.003941,
            580306709.879151, 488568, 0.03129944349480739, 0.2900402591673477,
            -0.870745062828064, 0.1281501054763794, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
