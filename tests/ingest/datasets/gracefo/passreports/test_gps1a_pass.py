import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.gps1a_pass import GraceFOGps1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOGps1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOGps1APassDataProduct
    expected_table_names = ['gracefo_gps1a_pass_04_c', 'gracefo_gps1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('GPS1A_2018-05-23_C_NEN_580381005.pass', 580305600, 607670745, 580381005.0,
            580391804.0, 99220, 0.1088400407180076, 0.3114384148664228,
            0.0, 1.0, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc)),
        ('GPS1A_2018-05-23_D_NEN_580358085.pass', 580305600, 607670764, 580358085.0,
            580364444.0, 58220, 0.10922551057215, 0.3119219428196791,
            0.0, 1.0, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
