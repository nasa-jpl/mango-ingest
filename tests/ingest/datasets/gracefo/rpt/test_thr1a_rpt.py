import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.thr1a_rpt import GraceFOThr1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOThr1ARptDataProduct
    expected_table_names = ['gracefo_thr1a_rpt_04_c', 'gracefo_thr1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('THR1A_2023-06-01_C_04.dat', 738849600, 738950159, 738838823.1,
        738946802.1, 582, 185.5917667238422, 198.0766668993374, 0.5, 1478.5,
        8, 0, 0, 582, 582, 0, 0, 0, 0,
         datetime(2023, 5, 31, 21, 0, 23, 100000, tzinfo=timezone.utc)),
        ('THR1A_2023-06-01_D_04.dat', 738849600, 738955940, 738838735.1,
         738946796.6, 555, 194.6043165467626, 242.3976301378986, 0.5, 1796,
         8, 0, 0, 555, 555, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 55, 100000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
