import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.act1b_rpt import GraceFOAct1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOAct1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOAct1BRptDataProduct
    expected_table_names = ['gracefo_act1b_rpt_04_c', 'gracefo_act1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int, int, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('ACT1B_2023-06-01_C_04.dat', 738849600, 739544469, 738849600, 738935999, 86400,
         1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         3.977978576477985e-09, 3.338788932881818e-09, 1.583010785550946e-09,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('ACT1B_2023-06-01_D_04.dat', 738849600, 739547194, 738849600, 738935999,
         86400, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         4.953952774876598e-09, 3.108925417196767e-09, 1.45852031841127e-09,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
