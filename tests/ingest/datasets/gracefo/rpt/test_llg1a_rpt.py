import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.llg1a_rpt import GraceFOLlg1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLlg1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOLlg1ARptDataProduct
    expected_table_names = ['gracefo_llg1a_rpt_04_c', 'gracefo_llg1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LLG1A_2023-06-01_C_04.dat', 738849600, 739854703, 738851656, 738919247,
         26, 3207.407407407407, 6690.043809075733, 0, 30675,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('LLG1A_2023-06-01_D_04.dat', 738849600, 739854704, 738878604, 738909810,
         8, 9622.222222222223, 13657.2101330134, 0, 31205,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
