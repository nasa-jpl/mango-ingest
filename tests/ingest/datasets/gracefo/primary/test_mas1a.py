import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.mas1a import GraceFOMas1ADataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase
from typing import Union


class GraceFOMas1ADatasetReaderTestCase(DatasetReaderTestCaseBase):
    dataset_cls = GraceFOMas1ADataProduct
    expected_table_names = ['gracefo_mas1a_04_c', 'gracefo_mas1a_04_d']

    expected_field_types = [int, int, str, str, str,
                            bool, bool, bool, bool, bool, bool, bool, bool, datetime,
                            Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)]
                            ]
    expected_table_row_counts = [24, 24]
    expected_table_first_rows = [
        (738849600,  0,  'C',  '00000000', '11000000',
         False, False, False, False, False, False, False, False, datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc),
         None, None, None, None, None,None,
         13.4351385452046, 13.45653098905893),
        (738849600,  0, 'D',  '00000000', '11000000',
         False, False, False, False, False, False, False, False, datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc),
         None, None, None, None, None, None,
         13.66316195230655, 13.82401085038554)
    ]
if __name__ == '__main__':
    unittest.main()
