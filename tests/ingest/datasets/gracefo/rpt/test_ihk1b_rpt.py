import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ihk1b_rpt import GraceFOIhk1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOIhk1BRptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOIhk1BRptDataProduct
    expected_table_names = ['gracefo_ihk1b_rpt_04_c', 'gracefo_ihk1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('IHK1B_2023-06-01_C_04.dat', 738849600, 739543833, 738849628.983328,
         738935968.982002, 28080, 3.074895782400907, 13.2302214659494, 0, 60,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 28, 983328, tzinfo=timezone.utc)),
        ('IHK1B_2023-06-01_D_04.dat', 738849600, 739543843, 738849620.982744,
         738935960.981369, 28080, 3.074895780656008, 13.2302214584417, 0, 60,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 20, 982744, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
