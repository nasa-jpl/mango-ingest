import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.ihk1a import GraceFOIhk1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOIhk1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOIhk1ADataProduct
    expected_table_names = ['gracefo_ihk1a_04_c', 'gracefo_ihk1a_04_d']
    expected_field_types = [int, int, str, str, str, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849629, 0, 'C', '00000000', 'T', 24.42157524342042, '21',
         datetime(2023, 6, 1, 0, 0, 29, tzinfo=timezone.utc)),
        (738849621, 0, 'D', '00000000', 'T', 25.63474238003979, '21',
         datetime(2023, 6, 1, 0, 0, 21, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
