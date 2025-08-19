import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ac11b_rpt import GraceFOAc11BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAc11BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAc11BRptDataProduct
    expected_table_names = ['gracefo_ac11b_rpt_04_c', 'gracefo_ac11b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int, int, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('AC11B_2023-07-01_C_04.dat', 741441600, 774498866, 741441600, 741527999, 86400,
         1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         1.49329432556621e-08, 6.350683787023124e-09, 4.949521269023811e-10,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc)),
        ('AC11B_2023-07-01_D_04.dat', 741441600 ,774499235, 741441600, 741527999, 86400,
         1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         3.517003758072372e-08, 2.383705832300845e-08, 1.226084404532019e-09,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
