import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.tnk1b_rpt import GraceFOTnk1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1BRptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOTnk1BRptDataProduct
    expected_table_names = ['gracefo_tnk1b_rpt_04_c', 'gracefo_tnk1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('TNK1B_2023-06-01_C_04.dat', 738849600, 739543742, 738849600.364329, 738935999.363001,
         178200, 0.4848455865184906, 0.4964432674209526, 0, 1.01199996471405,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 364329, tzinfo=timezone.utc)),
        ('TNK1B_2023-06-01_D_04.dat', 738849600, 739543708, 738849600.363744, 738935999.3623689,
         178200, 0.4848455862542486, 0.4964433113698147, 0, 1.010999917984009,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 363744, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
