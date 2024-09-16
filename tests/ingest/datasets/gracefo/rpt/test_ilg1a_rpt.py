import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ilg1a_rpt import GraceFOIlg1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOIlg1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOIlg1ARptDataProduct
    expected_table_names = ['gracefo_ilg1a_rpt_04_c', 'gracefo_ilg1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('ILG1A_2023-06-01_C_04.dat', 738849600, 738950167, 738849504,
         738936100, 46411, 1.86593695460128, 3.07184117765118, 0, 13,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023,  5, 31, 23, 58, 24, tzinfo=timezone.utc)),
        ('ILG1A_2023-06-01_D_04.dat', 738849600, 738955948, 738849501, 738936094,
         45429, 1.906249312113408, 3.086582562884725, 0, 13,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 58, 21, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
