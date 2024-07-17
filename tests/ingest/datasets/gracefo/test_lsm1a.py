import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.lsm1a import GraceFOLsm1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOLsm1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOLsm1ADataProduct
    expected_table_names = ['gracefo_lsm1a_04_c', 'gracefo_lsm1a_04_d']
    expected_field_types = [int, int, str, int, int, int, int, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 15480831, 'C', 1852, 1634, 27, 3872, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 100, tzinfo=timezone.utc)),
        (738849600, 12035298, 'D', 1824, 1573, 4093, 3807, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 100, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
