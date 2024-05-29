import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.ahk1b import GraceFOAhk1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOAhk1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOAhk1BDataProduct
    expected_table_names = ['gracefo_ahk1b_04_c', 'gracefo_ahk1b_04_d']

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
        (738849600,  91062,  'C',  '00000000', '00111100000000000000010001000000',
         datetime(2023, 6, 1, 0, 0, 0, 91062, tzinfo=timezone.utc),
         None, None, None, None, None,
         None, 9.984976053237915, None, None, None,
         0.001019239426, None, None, None, None,
         None, None, None, None, None,
         None, None, None, None, None,
         0, 0, 0,  '00000000000000000000000000000000'),
        (738849600,  94671, 'D',  '00000000',   '00111100000000000000010001000000',
         datetime(2023, 6, 1, 0, 0, 0, 94671, tzinfo=timezone.utc),
         None, None, None, None, None,
         None, 9.990030527114868, None, None, None,
         0.0003772974014, None, None, None, None,
         None, None, None, None, None,
         None, None, None, None, None,
         0, 0, 0, '00000000000000000000000000000000',)
    ]

    if __name__ == '__main__':
        unittest.main()
