import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.resids \
    import GraceFOResidsDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOResidsDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOResidsDataProduct
    expected_table_names = ['gracefo_resids_04_y']
    expected_field_types = [int, float, datetime]
    expected_table_row_counts = [101]
    expected_table_first_rows = [
        (738849600, -0.00778353,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
