import unittest
from datetime import datetime, timezone
from masschange.datasets.gracefo.imu1a import GraceFOImu1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOImu1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOImu1ADataset
    expected_table_names = ['gracefo_imu1a_c', 'gracefo_imu1a_d']
    expected_field_types = [str, int, float, str, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('C', 1, -3213.27944157619, '00000000', 738849600003919,
         datetime(2023, 6, 1, 0, 0, 0, 3919, tzinfo=timezone.utc)),
        ('D', 1, 694.8408306826, '00000000', 738849600003926,
         datetime(2023, 6, 1, 0, 0, 0, 3926, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
