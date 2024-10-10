import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.imu1b_rpt import GraceFOImu1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOImu1BRptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOImu1BRptDataProduct
    expected_table_names = ['gracefo_imu1b_rpt_04_c', 'gracefo_imu1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('IMU1B_2023-06-01_C_04.dat', 738849600, 739543834, 738849600.109131, 738935999.9881639,
         2073600, 0.041666628423803, 0.05734192830361966, 0.001106977462768555, 0.1266829967498779,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 109131, tzinfo=timezone.utc)),
        ('IMU1B_2023-06-01_D_04.dat', 738849600, 739544021, 738849600.108547, 738935999.987543,
         2073600, 0.04166662840598138, 0.05734206887725716, 0.001106977462768555, 0.1266580820083618,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 108547, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
