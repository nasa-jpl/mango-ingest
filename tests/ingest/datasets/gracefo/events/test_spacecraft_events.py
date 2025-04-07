import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.events.spacecraft_evnt import GraceFOSpacecraftEventsDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOSpacecraftEventsDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/events/'
    data_is_zipped = False

    dataset_cls = GraceFOSpacecraftEventsDataProduct
    expected_table_names = ['gracefo_spacecraft_evnt_04_c', 'gracefo_spacecraft_evnt_04_d']
    expected_field_types = [str, str, str, str, str,
                            str, datetime]
    expected_table_row_counts = [318, 320]
    expected_table_first_rows = [
        ('IPU','2018-07-20 08:05:34 GPS', 'Fri Jul 20 11:28:21','operator',
        'C','3 1 1216109134.0 1', datetime(2018, 7, 20, 8, 5, 34, tzinfo=timezone.utc)),
        ('IPU', '2018-07-18 18:08:14 GPS', 'Wed Jul 18 20:02:46', 'operator',
         'D', '3 1 1215972494.0 1', datetime(2018, 7, 18, 18, 8, 14, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
