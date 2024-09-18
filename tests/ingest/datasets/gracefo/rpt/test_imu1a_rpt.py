import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.imu1a_rpt import GraceFOImu1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOImu1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOImu1ARptDataProduct
    expected_table_names = ['gracefo_imu1a_rpt_04_c', 'gracefo_imu1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('IMU1A_2023-06-01_C_04.dat', 738849600, 738950154, 738838700.003939,
         738946899.8780299, 2596800, 0.0416666342257949, 0.05734193060033032,
         0.001106977462768555, 0.1266829967498779,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023,   5, 31, 20, 58, 20, 3939, tzinfo=timezone.utc)),
        ('IMU1A_2023-06-01_D_04.dat', 738849600, 738955935, 738838700.003943,
         738946899.8780431, 2596800, 0.04166663422932967, 0.05734207099833877,
         0.001106977462768555, 0.1266579627990723,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 5, 31, 20, 58, 20, 3943, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
