import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.thr1b_rpt import GraceFOThr1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFORbr1BRptDatasetDatasetIngestTestCaseBase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOThr1BRptDataProduct
    expected_table_names = ['gracefo_thr1b_rpt_04_c', 'gracefo_thr1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('THR1B_2023-06-01_C_04.dat', 738849600, 739543700, 738849620.083329, 738935819.082004,
         459, 188.2074206877484, 202.7798686623063, 0.5, 1478.499976992607,
         8, 0, 0, 459, 459, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 20, 83329, tzinfo=timezone.utc)),
        ('THR1B_2023-06-01_D_04.dat', 738849600, 739543740, 738849943.582739, 738935877.58137, 454,
         189.6997762273742, 236.7779711966128, 0.5, 1795.999971032143,
         8, 0, 0, 454, 454, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 5, 43, 582739, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
