import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.gps1b import GraceFOGps1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOGps1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOGps1BDataProduct
    expected_table_names = ['gracefo_gps1b_04_c', 'gracefo_gps1b_04_d']

    expected_field_types = [int, int, str, int, int, str, str, datetime,
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[int, type(None)], Union[int, type(None)], Union[int, type(None)],
                            Union[int, type(None)], Union[int, type(None)], Union[int, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)]
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600,  0,  'C',  6,  0,
         '0000111111111111',   '00000011',  datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
         21635397.06426055,  21635397.15109361,  21635399.2897599,
         21635396.93097395,  21635397.02641411,  21635399.06920046,
         387,  569,  1064,  0,  1,  2,
         None, None, None, None),
        (738849600,  0,  'D',  6,  0,
         '0000111111111111',   '00000011',  datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
         21723240.35458767, 21723240.33119728, 21723240.53983342,
         21723240.32211612, 21723240.22568756, 21723240.42391783,
         399,  559,  957,  24,  25,  26,
         None, None, None, None)
    ]
if __name__ == '__main__':
    unittest.main()
