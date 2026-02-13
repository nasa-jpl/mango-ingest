import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.ddic import GraceFODdicDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFODdicDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFODdicDataProduct
    expected_table_names = ['gracefo_ddic_00_y']
    expected_field_types = [float, float, str, datetime]
    expected_table_row_counts = [200]
    expected_table_first_rows = [
        (739022409.0, -0.00019782409071922302, '000',
         datetime(2023, 6, 3, 0, 0, 9, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
