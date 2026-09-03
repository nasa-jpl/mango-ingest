import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.sca1a_pass import GraceFOSca1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOSca1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOSca1APassDataProduct
    expected_table_names = ['gracefo_sca1a_pass_04_c', 'gracefo_sca1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('SCA1A_2018-05-23_C_NEN_580381007.pass', 580305600, 607670744, 580381007.529975,
            580391807.02999, 64800, 0.1666615227843201, 0.2354732182587755,
            -0.1250159740447998, 0.6250159740447998, 8, 3985, 0, 0, 0, 0, 0, 64734,
            60815, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc)),
        ('SCA1A_2018-05-23_D_NEN_580358088.pass', 580305600, 607670764, 580358088.529986,
            580364448.029992, 38160, 0.1666579314437083, 0.233766355641128,
            -0.125, 0.6250160932540894, 8, 3292, 0, 0, 0, 0, 0, 38081,
            34869, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
