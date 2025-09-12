import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.ddic import GraceFODdicDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFODdicDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFODdicDataProduct
    expected_table_names = ['gracefo_ddic_00_y']
    expected_field_types = [float, float, datetime]
    expected_table_row_counts = [101,101]
    expected_table_first_rows = [
        (738849609.0, -0.00024859607219696045,
         datetime(2023, 6, 1, 0, 0, 9, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
