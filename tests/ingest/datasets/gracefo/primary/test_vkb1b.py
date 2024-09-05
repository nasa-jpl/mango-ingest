import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.vkb1b import GraceFOVkb1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOVkb1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOVkb1BDataProduct
    expected_table_names = ['gracefo_vkb1b_04_c', 'gracefo_vkb1b_04_d']
    expected_field_types = [int, str, float, float, float, float, str, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        (580219200, 'C', 1.4444, 1, -0.00012, 0.00031,
         '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (580219200, 'D', 1.44446, 1, 4e-05, 0.00016,
         '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
