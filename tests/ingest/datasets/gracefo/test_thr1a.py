import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.thr1a import GraceFOThr1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOThr1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOThr1ADataset
    expected_table_names = ['gracefo_thr1a_c', 'gracefo_thr1a_d']
    expected_field_types = [int, int, str,
                            int, int, int, int, int, int,
                            int, int, int, int, int, int,
                            int, int, int, int, int, int, int, int, int, int, int, int, int, int,
                            int, int, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849620, 100000, 'C',
         328319, 7984, 191171, 6503, 171194, 121745,
         327957, 7814, 191158, 6440, 171084, 121286,
         50, 0, 0, 0, 0, 0, 50, 0, 0, 0, 0, 0, 0, 0,
         114098563, 29162000,'00001100',
         datetime(2023, 6, 1, 0, 0, 20, 100000, tzinfo=timezone.utc)),
        (738849943, 600000, 'D', 228691, 6246, 257729, 3828, 132419, 144198,
         228603, 5832, 257471, 3726, 132320, 143566,
         0, 0, 50, 0, 0, 0, 0, 0, 50, 0, 0, 0, 0, 0,
         106517163, 27414000, '00001100',
         datetime(2023, 6, 1, 0, 5, 43, 600000, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
