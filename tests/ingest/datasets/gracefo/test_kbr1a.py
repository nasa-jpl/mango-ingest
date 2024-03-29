import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.kbr1a import GraceFOKbr1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOKbr1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOKbr1ADataset
    expected_table_names = ['gracefo_kbr1a_c', 'gracefo_kbr1a_d']

    expected_field_types = [int, int, str, int, int, str, str, datetime,
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[int, type(None)], Union[int, type(None)], Union[int, type(None)],
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)],
                            Union[int, type(None)], Union[int, type(None)]
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600,  50000,  'C',  51, 9, '0011000000000000', '00000000', datetime(2023, 6, 1, 0, 0, 0, 50000, tzinfo=timezone.utc),
         None, None, None,
         None, None, None,
         None, None, None,
         None, None, None,
         -91268195.43288438, -87353114.99209328,
         None, None
        ),

        (738849600,  50000, 'D',  50, 11, '0011000000000000',   '00000000', datetime(2023, 6, 1, 0, 0, 0, 50000, tzinfo=timezone.utc),
         None, None, None,
         None, None, None,
         None, None, None,
         None, None, None,
         47747299.80598602, 91147677.24506988,
         None, None)
    ]

    if __name__ == '__main__':
        unittest.main()
