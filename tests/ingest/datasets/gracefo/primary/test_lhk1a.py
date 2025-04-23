import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.lhk1a import GraceFOLhk1ADataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLhk1ADatasetReaderTestCase(DatasetReaderTestCaseBase):
    dataset_cls = GraceFOLhk1ADataProduct
    expected_table_names = ['gracefo_lhk1a_04_c', 'gracefo_lhk1a_04_d']
    expected_field_types = [int, int, str, str, str, str, int, str,
                            bool, bool, bool, bool, bool, bool, bool, bool, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 52670038, 'S', 'C', '00000000', '?', 1905, 'TMA_LASER_POWER',
         False, False, False, False, False, False, False, False,
         datetime(2023, 6, 1, 0, 0, 0, 52670, tzinfo=timezone.utc)),
        (738849600, 12986334, 'S', 'D', '00000000', '?', 1908, 'TMA_LASER_POWER',
         False, False, False, False, False, False, False, False,
         datetime(2023, 6, 1, 0, 0, 0, 12986, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
