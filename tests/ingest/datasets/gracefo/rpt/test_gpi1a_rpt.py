import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.gpi1a_rpt import GraceFOGpi1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOGpi1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOGpi1ARptDataProduct
    expected_table_names = ['gracefo_gpi1a_rpt_04_c', 'gracefo_gpi1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, int, float, int, float, int,
                            int, int, int, int, int, int, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('GPI1A_2023-06-01_C_04.dat', 738849600, 738951133, 738838750, 738946890,
         97791, 1.105839042846917, 3.136161736866354, 0, 10,
         8, 831, 831, 0, 0, 0, 0, 0, 0,
         0.002912867243604649, 97791, 0.003272339665548425, 97791,
         0.00480367615456511, 97791, 902, 52, 52, 0, 52, 989242,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
         ('GPI1A_2023-06-01_D_04.dat', 738849600, 738956836, 738838750,
          738946890, 98600, 1.096765687278776, 3.124861900947892, 0, 10,
          8, 835, 835, 0, 0, 0, 0, 0, 0,
          0.002850508044715185, 98600, 0.003259979466418562, 98600,
          0.004705693421750409, 98600, 891, 55, 55, 0, 55, 998813,
          datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
