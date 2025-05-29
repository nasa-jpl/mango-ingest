import unittest
from datetime import datetime, timezone
from typing import Union
from masschange.dataproducts.implementations.gracefo.events.soe_evnt import GraceFOSoeEventsDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOSoeEventsDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/events/'
    data_is_zipped = False

    dataset_cls = GraceFOSoeEventsDataProduct
    expected_table_names = ['gracefo_soe_evnt_04_c', 'gracefo_soe_evnt_04_d']
    expected_field_types = [str, str, float, Union[str, None], Union[str, None], str,
                            str, str, datetime]
    expected_table_row_counts = [1491, 1405]
    expected_table_first_rows = [
        ('ACC', '2000-01-01 12:00:00 GPS', 0.0,  None, None, 'Initial default by hywen',
        'C', '1 1', datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
        ('MTEB', '2000-01-01 12:00:00 GPS', 0, None, None, 'Initial default by hywen',
        'D', '3 0.0 0.0 0.0', datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
