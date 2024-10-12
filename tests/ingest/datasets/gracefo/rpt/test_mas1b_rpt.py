import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.mas1b_rpt import GraceFOMas1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFORbr1BRptDatasetDatasetIngestTestCaseBase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOMas1BRptDataProduct
    expected_table_names = ['gracefo_mas1b_rpt_04_c', 'gracefo_mas1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('MAS1B_2023-06-01_C_04.dat', 738849600, 739858874, 738853199.983274, 738935999.9820009,
         24, 3599.999944650609, 0, 3599.999943971634, 3599.999945044518,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 59, 59, 983274, tzinfo=timezone.utc)),
        ('MAS1B_2023-06-01_D_04.dat', 738849600, 739859077, 738853199.982687, 738935999.9813679,
         24, 3599.999942649966, 0, 3599.999941945076, 3599.99994301796,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 59, 59, 982687, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
