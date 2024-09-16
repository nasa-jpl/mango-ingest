import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ahk1a_rpt import GraceFOAhk1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOAhk1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOAhk1ARptDataProduct
    expected_table_names = ['gracefo_ahk1a_rpt_04_c', 'gracefo_ahk1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('AHK1A_2023-06-01_C_04.dat', 738849600, 738950136, 738849300.034633,
         738936299.935747, 870078, 0.09999103655651967, 7.708327801051128e-06,
         0.09996688365936279, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 0, 34633, tzinfo=timezone.utc)),
        ('AHK1A_2023-06-01_D_04.dat', 738849600, 738955919, 738849300.03819,
         738936299.923788, 870076, 0.09999124856816281, 7.073838774653251e-06,
         0.09996891021728516, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 0, 38190, timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
