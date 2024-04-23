import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.tnk1a import GraceFOTnk1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOTnk1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOTnk1ADataset
    expected_table_names = ['gracefo_tnk1a_04_c', 'gracefo_tnk1a_04_d']

    expected_field_types = [int, int, str, int, str, str, datetime,
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)]
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 381000, 'C', 1, '00000000', '00000011', datetime(2023, 6, 1, 0, 0, 0, 381000, tzinfo=timezone.utc),
         221.8955993652344, 1.507822036743164, None, None, None, None, None),

        (738849600, 381000, 'D', 1, '00000000', '00000011', datetime(2023, 6, 1, 0, 0, 0, 381000, tzinfo=timezone.utc),
         224.9555053710938, 1.602975010871887, None, None, None, None, None)
    ]

    if __name__ == '__main__':
        unittest.main()
