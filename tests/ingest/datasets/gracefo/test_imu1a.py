import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.imu1a import GraceFOImu1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOImu1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOImu1ADataProduct
    expected_table_names = ['gracefo_imu1a_04_c', 'gracefo_imu1a_04_d']
    expected_field_types = [int, int, str, int, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 3919, 'C', 1, -3213.27944157619, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 3919, tzinfo=timezone.utc)),
        (738849600, 3926, 'D', 1, 694.8408306826, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 3926, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
