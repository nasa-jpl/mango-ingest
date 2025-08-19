import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ac01a_rpt import GraceFOAc01ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAc01ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAc01ARptDataProduct
    expected_table_names = ['gracefo_ac01a_rpt_04_c', 'gracefo_ac01a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int,
                            int, int, int, int, int, int, int,
                            int, int, int, int, int, int, int,int, int,
                            int, int, int, int, int, int, int,int, int,
                            int, int, int, int, int, int, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('AC01A_2023-07-01_C_04.dat', 741441600, 774498558, 741430700.027781, 741538899.9207,
         1082097, 0.09999102937164943, 7.731896441047157e-06, 0.09996688365936279, 0.09999406337738037,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0,
         55, 50, 1626, 4, 60, 1956, 9, 50, 898,
         4, 63, 1288, 11, 50, 79, 14, 50, 51,
         0, 0, 0, 0, 0, 0,
         datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc)),
        ('AC01A_2023-07-01_D_04.dat', 741441600, 774498944, 741430700.001701, 741538899.927047,
         1082095, 0.09999124414886035, 7.091781385804344e-06, 0.09996891021728516, 0.09999406337738037,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         0, 0, 0, 0, 0, 0, 0,
         186, 50, 600, 69, 291, 1860, 192, 50,
         600, 87, 74, 11, 88, 108, 50, 520, 0, 0,
         0, 0, 0, 0, 0, 0,

         datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
