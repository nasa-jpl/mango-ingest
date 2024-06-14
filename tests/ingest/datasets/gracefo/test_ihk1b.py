import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.ihk1b import GraceFOIhk1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOIhk1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOIhk1BDataProduct
    expected_table_names = ['gracefo_ihk1b_04_c', 'gracefo_ihk1b_04_d']
    expected_field_types = [int, int, str, str, str, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849628, 983328, 'C', '00000000', 'T', 24.42157524342042, '21',
         datetime(2023, 6, 1, 0, 0, 28, 983328, tzinfo=timezone.utc)),
        (738849620, 982744, 'D', '00000000', 'T', 25.63474238003979, '21',
         datetime(2023, 6, 1, 0, 0, 20, 982744, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
