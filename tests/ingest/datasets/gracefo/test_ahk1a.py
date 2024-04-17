import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.ahk1a import GraceFOAhk1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOAhk1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOAhk1ADataset
    expected_table_names = ['gracefo_ahk1a_04_c', 'gracefo_ahk1a_04_d']

    expected_field_types = [int, int, str, str, str, datetime,
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            int, int, int, str
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600,  7739,  'C',  '00000000', '00111100000000000000010001000000', datetime(2023, 6, 1, 0, 0, 0, 7739, tzinfo=timezone.utc),

         None, None, None, None, None,
         None, 9.98496413230896, None, None, None,
         0.001013875008, None, None, None, None,
         None, None, None, None, None,
         None, None, None, None, None,
         0, 0, 0,  '00000000000000000000000000000000'),


        (738849600,  11933, 'D',  '00000000',   '00111100000000000000010001000000', datetime(2023, 6, 1, 0, 0, 0, 11933, tzinfo=timezone.utc),
         None, None, None, None, None,
         None, 9.990012645721436, None, None, None,
         0.0003808736801,None, None, None, None,
         None, None, None, None, None,
         None, None, None, None, None,
         0, 0, 0, '00000000000000000000000000000000',)
    ]

    if __name__ == '__main__':
        unittest.main()
