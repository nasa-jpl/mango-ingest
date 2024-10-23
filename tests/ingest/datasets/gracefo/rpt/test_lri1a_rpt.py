import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.lri1a_rpt import GraceFOLri1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFORLri1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOLri1ARptDataProduct
    expected_table_names = ['gracefo_lri1a_rpt_04_c', 'gracefo_lri1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LRI1A_2023-06-01_C_04.dat', 738849600, 738950140, 738849300.0822481,
         738936299.9787712, 840768, 0.1034768211919807, 1.22212802636059e-07,
         0.1034767627716064, 0.103476881980896, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 0, 82248, tzinfo=timezone.utc)),
        ('LRI1A_2023-06-01_D_04.dat', 738849600, 738955923, 738849300.0273725,
         738936299.9005125, 840785, 0.103474701159843, 2.503647301057917e-07,
         0.1034746170043945, 0.1034747362136841, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 0, 27372, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
