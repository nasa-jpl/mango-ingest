import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.events.spacecraft_evnt import GraceFOSpacecraftEventsDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOSpacecraftEventsDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/events/'
    data_is_zipped = False

    dataset_cls = GraceFOSpacecraftEventsDataProduct
    expected_table_names = ['gracefo_spacecraft_evnt_04_y']
    expected_field_types = [str, str, str, str, str,
                            str, datetime]
    expected_table_row_counts = [3]
    expected_table_first_rows = [
        ('IPUR','2020-12-22 10:57:05 GPS', 'Mon Dec 21 14:14:36 2021','operator',
        'C','3  1 1292583425.0 1', datetime(2020, 12, 22, 10, 57, 5, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
