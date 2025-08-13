import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ac11a_rpt import GraceFOAc11ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAc11ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAc11ARptDataProduct
    expected_table_names = ['gracefo_ac11a_rpt_04_c', 'gracefo_ac11a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int,
                            int, int, int, int, int, int, int,
                            int, int, int, int, int, int, int,int, int,
                            int, int, int, int, int, int, int,int, int,
                            int, int, int, int, int, int, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('AC11A_2023-07-01_C_04.dat', 741441600, 774498750 ,741430700.027781, 741538899.9207,
         1082097, 0.09999102937164943, 7.731896441047157e-06, 0.09996688365936279, 0.09999406337738037,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0,
         345, 50, 162, 687, 60, 195, 6108, 50, 89,
         89, 9, 63, 128, 833, 50, 79, 72, 50,
         510, 0, 0, 0, 0, 0,
         datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc)),
        ('AC11A_2023-07-01_D_04.dat', 741441600, 774499099, 741430700.001701, 741538899.927047,
         1082095, 0.09999124414886035, 7.091781385804344e-06, 0.09996891021728516, 0.09999406337738037,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0,
         186, 50, 600, 69, 291, 1860, 192, 50, 600,
         87, 74, 118, 810, 8, 50, 520, 0, 0,
         0, 0, 0, 0, 0, 0,
         datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
