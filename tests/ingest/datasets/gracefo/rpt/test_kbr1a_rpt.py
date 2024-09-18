import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.kbr1a_rpt import GraceFOKbr1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOKbr1ARptDataProduct
    expected_table_names = ['gracefo_kbr1a_rpt_04_c', 'gracefo_kbr1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('KBR1A_2023-06-01_C_04.dat', 738849600, 738950166, 738849300.05, 738936299.95,
         870000, 0.1000000000001096, 2.179573407564537e-07, 0.09999990463256836, 0.1000000238418579,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 0, 50000, tzinfo=timezone.utc)),
        ('KBR1A_2023-06-01_D_04.dat', 738849600, 738955947, 738849300.05, 738936299.95, 870000,
         0.1000000000001096, 2.179573407564537e-07, 0.09999990463256836, 0.1000000238418579,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 0, 50000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
