import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.vgo1b import GraceFOVgo1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOVgo1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOVgo1BDataProduct
    expected_table_names = ['gracefo_vgo1b_04_c', 'gracefo_vgo1b_04_d']
    expected_field_types = [int, str, float, float, float, float, str, datetime]
    expected_table_row_counts = [2, 2]
    expected_table_first_rows = [
        (580219200, 'C', 1.572768756842531, -0.9926761281387254, 0, -0.120806062031294,
         '00000001', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (580219200, 'D', 1.572768756842531, -0.9926761281387254, 0, -0.120806062031294,
         '00000001', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
