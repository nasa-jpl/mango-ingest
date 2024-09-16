import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.tnk1a_rpt import GraceFOTnk1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOTnk1ARptDataProduct
    expected_table_names = ['gracefo_tnk1a_rpt_04_c', 'gracefo_tnk1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('TNK1A_2023-06-01_C_04.dat', 738849600, 738951395, 738849600.381, 738935999.381,
         178200, 0.4848455939707855, 0.4964432750527839, 0, 1.01199996471405, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023,  5, 31, 21, 0, 23, 100000, tzinfo=timezone.utc)),
        ('TNK1A_2023-06-01_D_04.dat', 738849600, 738956277, 738849600.381,
         738935999.381, 178200, 0.4848455939707855, 0.4964433192815519, 0,
         1.010999917984009, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 381000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
