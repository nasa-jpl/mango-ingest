import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.gnv1a_rpt import GraceFOGnv1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOGnv1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOGnv1ARptDataProduct
    expected_table_names = ['gracefo_gnv1a_rpt_04_c', 'gracefo_gnv1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('GNV1A_2023-06-01_C_04.dat', 738849600, 738950159, 738838700,
         738946900, 54101, 2, 0, 2, 2, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 20, tzinfo=timezone.utc)),
        ('GNV1A_2023-06-01_D_04.dat', 738849600, 738955940, 738838700, 738946900,
         54101, 2, 0, 2, 2, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 20, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
