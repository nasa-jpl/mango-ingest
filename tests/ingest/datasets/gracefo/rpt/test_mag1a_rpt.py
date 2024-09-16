import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.mag1a_rpt import GraceFOMag1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOMag1ARptDataProduct
    expected_table_names = ['gracefo_mag1a_rpt_04_c', 'gracefo_mag1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('MAG1A_2023-06-01_C_04.dat', 738849600, 738951393, 738838700.1,
         738946899.6, 216400, 0.5, 0, 0.5, 0.5,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58,  20, 100000, tzinfo=timezone.utc)),
        ('MAG1A_2023-06-01_D_04.dat', 738849600, 738956886, 738838700.1,
         738946899.6, 216400, 0.5, 0, 0.5, 0.5,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 20, 100000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
