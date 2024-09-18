import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ihk1a_rpt import GraceFOIhk1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOIhk1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOIhk1ARptDataProduct
    expected_table_names = ['gracefo_ihk1a_rpt_04_c', 'gracefo_ihk1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('IHK1A_2023-06-01_C_04.dat', 738849600, 738950168, 738849629, 738935969,
         28080, 3.0768135037926, 13.23170434341855, 0, 60,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 29, tzinfo=timezone.utc)),
        ('IHK1A_2023-06-01_D_04.dat', 738849600, 738955949, 738849621,
         738935961, 28080, 3.0768135037926, 13.23191965039186, 0, 60,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 21, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
