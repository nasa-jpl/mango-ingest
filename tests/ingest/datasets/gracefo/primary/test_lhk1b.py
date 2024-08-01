import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.lhk1b import GraceFOLhk1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOLhk1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOLhk1BDataProduct
    expected_table_names = ['gracefo_lhk1b_04_c', 'gracefo_lhk1b_04_d']
    expected_field_types = [int, int, str, str, str, int, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 138320886, 'C', '00000000', '?', 1905, 'TMA_LASER_POWER',
         datetime(2023, 6, 1, 0, 0, 0, 138321, tzinfo=timezone.utc)),
        (738849600, 946483437, 'D', '00000000', '?', 1904, 'TMA_LASER_POWER',
         datetime(2023, 6, 1, 0, 0, 0, 946483, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
