import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.pci1a_rpt import GraceFOPci1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOPci1ARptDataProduct
    expected_table_names = ['gracefo_pci1a_rpt_04_c', 'gracefo_pci1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('PCI1A_2023-06-01_C_04.dat', 738849600, 739546756, 738849340,
         738936260, 17385, 5, 0, 5, 5,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 40, tzinfo=timezone.utc)),
        ('PCI1A_2023-06-01_D_04.dat', 738849600, 739546153, 738849340,
         738936260, 17385, 5, 0, 5, 5,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 40, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
