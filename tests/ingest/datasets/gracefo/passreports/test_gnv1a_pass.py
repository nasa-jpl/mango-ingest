import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.gnv1a_pass import GraceFOGnv1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOGnv1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOGnv1APassDataProduct
    expected_table_names = ['gracefo_gnv1a_pass_04_c', 'gracefo_gnv1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('GNV1A_2018-05-23_C_NEN_580381008.pass', 580305600, 607670745, 580381008.0,
            580391806.0, 5400, 2.0, 0.0,
            2.0, 2.0, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc)),
        ('GNV1A_2018-05-23_D_NEN_580358088.pass', 580305600, 607670764, 580358088.0,
            580364446.0, 3180, 2.0, 0.0,
            2.0, 2.0, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
