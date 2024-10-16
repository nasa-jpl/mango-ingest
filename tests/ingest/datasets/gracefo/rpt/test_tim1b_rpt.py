import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.tim1b_rpt import GraceFOTim1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFORbr1BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOTim1BRptDataProduct
    expected_table_names = ['gracefo_tim1b_rpt_04_c', 'gracefo_tim1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('TIM1B_2023-06-01_C_04.dat', 738849600, 738951394, 738838701, 738946893, 13525,
         7.999926062846581, 0.008598353721148632, 7,
         8, 8, 0, 0, 13525, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 21, tzinfo=timezone.utc)),
        ('TIM1B_2023-06-01_D_04.dat', 738849600, 738956277, 738838707, 738946899, 13525,
         7.999926062846581, 0.008598353721148632, 7,
         8, 8, 0, 0, 13525, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 27, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
