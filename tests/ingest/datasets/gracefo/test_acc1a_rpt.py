import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.acc1a_rpt import GraceFOAcc1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOAcc1ARptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOAcc1ARptDataProduct
    expected_table_names = ['gracefo_acc1a_rpt_04_c', 'gracefo_acc1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('ACC1A_2023-06-01_C_04.dat', 738849600, 738950131, 738838700.084855, 738946899.985532,
         1082097, 0.09999103654119867, 7.713331636041786e-06, 0.09996688365936279, 0.09999406337738037,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
        ('ACC1A_2023-06-01_D_04.dat', 738849600, 738955915, 738838700.065924, 738946899.996034,
         1082095, 0.09999124855139915, 7.079683276725009e-06, 0.09996891021728516, 0.09999406337738037,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
