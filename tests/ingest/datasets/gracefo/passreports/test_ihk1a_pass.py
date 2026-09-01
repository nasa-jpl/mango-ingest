import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.ihk1a_pass import GraceFOIhk1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOIhk1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOIhk1APassDataProduct
    expected_table_names = ['gracefo_ihk1a_pass_04_c', 'gracefo_ihk1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('IHK1A_2018-05-23_C_NEN_580381066.pass', 580305600, 607670745, 580381066.0,
            580391806.0, 3510, 3.060701054431462, 13.20129434265026,
            0.0, 60.0, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc)),
        ('IHK1A_2018-05-23_D_NEN_580358129.pass', 580305600, 607670764, 580358129.0,
            580364429.0, 2067, 3.049370764762827, 13.17814796641696,
            0.0, 60.0, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 23, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
