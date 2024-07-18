import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.imu1b import GraceFOImu1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOImu1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOImu1BDataProduct
    expected_table_names = ['gracefo_imu1b_04_c', 'gracefo_imu1b_04_d']
    expected_field_types = [int, int, str, int, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 109131, 'C', 1, -3213.27313354009, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 109131, tzinfo=timezone.utc)),
        (738849600, 108547, 'D', 1, 694.834522646494, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 108547, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
