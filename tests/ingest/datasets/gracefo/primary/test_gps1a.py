import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.gps1a import GraceFOGps1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOGps1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOGps1ADataProduct
    expected_table_names = ['gracefo_gps1a_04_c', 'gracefo_gps1a_04_d']

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
         '0000111111111111',  '00000000', datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
         26633347.66090525,  26633347.74773831,  26633349.88631521,
         -2800344.580491981,  -2800344.484944549,  -2949069.922217668,
         387,  569,  1064,  0,  1,  2,
         None, None, None, None),
        (738849600,  0,  'D',  3,  0,
         '0000111111111111',   '00000000', datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
         28906734.59511109,  28906789.57587641, 28906819.03373108,
         - 1441909.611541173, - 1441909.513857249, - 1590532.967708466,
         156,  19,  15,  36,  37,  38,
         None, None, None, None)
    ]
if __name__ == '__main__':
    unittest.main()
