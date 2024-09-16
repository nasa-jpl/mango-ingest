import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.gps1a_rpt import GraceFOGps1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOGps1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOGps1ARptDataProduct
    expected_table_names = ['gracefo_gps1a_rpt_04_c', 'gracefo_gps1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('GPS1A_2023-06-01_C_04.dat', 738849600, 738950162, 738838700,
         738946900, 1055570, 0.1025039575811719, 0.3033099013572903,
         0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 20, tzinfo=timezone.utc)),
        ('GPS1A_2023-06-01_D_04.dat', 738849600, 738955943, 738838700,
         738946900, 1081525, 0.1000440119682966, 0.3000586736583096,
         0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 20, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
