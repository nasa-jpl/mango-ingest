import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.vsl1b import GraceFOVsl1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOVsl1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOVsl1BDataProduct
    expected_table_names = ['gracefo_vsl1b_04_c', 'gracefo_vsl1b_04_d']
    expected_field_types = [int, str, float, float, float, float, str, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        (580219200, 'C', 0.7183, -0.8353, -0.4559, 0.3074,
         '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (580219200, 'D', 0.7183, -0.8353, -0.4559, 0.3074,
         '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
