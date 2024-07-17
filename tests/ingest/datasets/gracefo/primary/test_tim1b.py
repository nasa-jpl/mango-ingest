import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.tim1b import GraceFOTim1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOTim1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOTim1BDataProduct
    expected_table_names = ['gracefo_tim1b_04_c', 'gracefo_tim1b_04_d']
    expected_field_types = [int, str, int, int, int, int, int,
                            str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849605, 'C', 0, 738849605, 0, -1, -1, '00000100',
         datetime(2023, 6, 1, 0, 0, 5, tzinfo=timezone.utc)),
        (738849603, 'D', 0, 738849603, 119, -1, -1, '00000100',
         datetime(2023, 6, 1, 0, 0, 3, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
