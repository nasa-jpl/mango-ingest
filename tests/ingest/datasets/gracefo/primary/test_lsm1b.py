import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.lsm1b import GraceFOLsm1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOLsm1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOLsm1BDataProduct
    expected_table_names = ['gracefo_lsm1b_04_c', 'gracefo_lsm1b_04_d']
    expected_field_types = [int, int, str, float, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 101131761, 'C', 1001.392361856, -542.8372193686176, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 101132, tzinfo=timezone.utc)),
        (738849600, 49007136, 'D', 1156.737105839998, -827.1325656396168, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 49007, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
