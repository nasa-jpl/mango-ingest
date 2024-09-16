import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.mas1a_rpt import GraceFOMas1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOMas1ARptDataProduct
    expected_table_names = ['gracefo_mas1a_rpt_04_c', 'gracefo_mas1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('MAS1A_2023-06-01_C_04.dat', 738849600, 739858849, 738846000,
         738939600, 27, 3600, 0, 3600, 3600,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 0, tzinfo=timezone.utc)),
        ('MAS1A_2023-06-01_D_04.dat', 738849600, 739858991, 738846000,
         738939600, 27, 3600, 0, 3600, 3600,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
