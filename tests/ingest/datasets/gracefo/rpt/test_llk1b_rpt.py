import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.llk1b_rpt import GraceFOLlk1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFORbr1BRptDatasetReaderTestCase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOLlk1BRptDataProduct
    expected_table_names = ['gracefo_llk1b_rpt_04_c', 'gracefo_llk1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, float, float, float, float, float, int,
                            float, float, float, float, float, float, int,
                            int, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LLK1B_2023-06-01_C_04.dat', 738849600, 739547521, 738849300, 738936300,
         8703, 9.997701677775224, 0.1515847616441534, 0, 10,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         -7.240280207980679e-05, 1.123121174362387e-07,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc)),
        ('LLK1B_2023-06-01_D_04.dat', 738849600, 739547521, 738849300, 738936300, 8703,
         9.997701677775224, 0.1515847616441534, 0, 10,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         -7.240280207980679e-05, 1.123121174362387e-07,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
