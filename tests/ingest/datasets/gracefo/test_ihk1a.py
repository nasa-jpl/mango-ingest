import unittest
from datetime import datetime, timezone
from masschange.datasets.gracefo.ihk1a import GraceFOIhk1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOIhk1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOIhk1ADataset
    expected_table_names = ['gracefo_ihk1a_c', 'gracefo_ihk1a_d']
    expected_field_types = [str, str, str, float, str, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('C', '00000000', 'T', 24.42157524342042, '21', 738849629000000,
         datetime(2023, 6, 1, 0, 0, 29, tzinfo=timezone.utc)),
        ('D', '00000000', 'T', 25.63474238003979, '21', 738849621000000,
         datetime(2023, 6, 1, 0, 0, 21, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
