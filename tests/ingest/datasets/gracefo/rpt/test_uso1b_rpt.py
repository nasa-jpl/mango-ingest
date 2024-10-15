import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.uso1b_rpt import GraceFOUso1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFORbr1BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOUso1BRptDataProduct
    expected_table_names = ['gracefo_uso1b_rpt_04_c', 'gracefo_uso1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('USO1B_2023-06-01_C_04.dat', 738849600, 739543742, 738849300, 738936300,
         2, 87000, 0, 87000, 87000,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc)),
        ('USO1B_2023-06-01_D_04.dat', 738849600, 739543742, 738849300, 738936300,
         2, 87000, 0, 87000, 87000,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
