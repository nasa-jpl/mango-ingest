import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ahk1b_rpt import GraceFOAhk1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOAhk1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOAhk1BRptDataProduct
    expected_table_names = ['gracefo_ahk1b_rpt_04_c', 'gracefo_ahk1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('AHK1B_2023-06-01_C_04.dat', 738849600, 739543748, 738849600.0910619,
         738935999.944638, 864077, 0.09999103502014188, 7.709171449973115e-06,
         0.09996592998504639, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 91062, tzinfo=timezone.utc)),
        ('AHK1B_2023-06-01_D_04.dat', 738849600, 739543747, 738849600.094671,
         738935999.93141, 864075, 0.09999124697531005, 7.074717504815308e-06,
         0.09996795654296875, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 94671, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
