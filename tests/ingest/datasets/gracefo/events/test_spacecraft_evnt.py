import unittest
from datetime import datetime, timezone
from typing import Union
from masschange.dataproducts.implementations.gracefo.events.spacecraft_evnt import GraceFOSpacecraftEventsDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOSpacecraftEventsDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/events/'
    data_is_zipped = False
    maxDiff = None  # Prevent unittest.TestCase from truncating the diff output
    dataset_cls = GraceFOSpacecraftEventsDataProduct
    expected_table_names = ['gracefo_spacecraft_evnt_04_c', 'gracefo_spacecraft_evnt_04_d']
    expected_field_types = [str, str, float, Union[str, None], Union[str, None], str,
                            str, datetime]
    expected_table_row_counts = [843, 843]
    expected_table_first_rows = [
        ('comment', '2018-05-23 00:00:00',  580305600.0, None, None, 'C',
            '           GRACE-C/D Missing data in the beginning of the arc; pass files '
            + 'show this as well. Changed start time \n                     to the nearest '
            + '5-min epoch from the largest first time on the quaternions and initLeo (nav '
            + 'sol).\n                     looks good; except possibly higher than normal '
            + 'number of outliers. This is early data; we should \n                     '
            + 'look closer later.\n           IPU resets manually added at 04:21:00.0 for '
            + 'GRACE-C and 04:56:10.0 for GRACE-D\n           Time gaps of up to 27 '
            + 'seconds in IMU; MAG; TIM; and TNK near time syncs to GPS: GRACE-C at 08:09; '
            + 'GRACE-D at 09:52\n', datetime(2018, 5, 23, 0, 0, 0, tzinfo=timezone.utc)),

        ('comment', '2018-05-23 00:00:00',  580305600.0, None, None, 'D',
            '           GRACE-C/D Missing data in the beginning of the arc; pass files '
            + 'show this as well. Changed start time \n                     to the nearest '
            + '5-min epoch from the largest first time on the quaternions and initLeo (nav '
            + 'sol).\n                     looks good; except possibly higher than normal '
            + 'number of outliers. This is early data; we should \n                     '
            + 'look closer later.\n           IPU resets manually added at 04:21:00.0 for '
            + 'GRACE-C and 04:56:10.0 for GRACE-D\n           Time gaps of up to 27 '
            + 'seconds in IMU; MAG; TIM; and TNK near time syncs to GPS: GRACE-C at 08:09; '
            + 'GRACE-D at 09:52\n', datetime(2018, 5, 23, 0, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
