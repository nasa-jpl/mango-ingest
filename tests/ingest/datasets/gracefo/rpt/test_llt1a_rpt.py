import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.llt1a_rpt import GraceFOLlt1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLlt1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOLlt1ARptDataProduct
    expected_table_names = ['gracefo_llt1a_rpt_04_y']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LLT1A_2023-06-01_Y_04.dat', 738849600, 739547109, 738847800 ,738937800,
         180002, 0.4999972222376542, 0.499999999992284, 0, 1,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
