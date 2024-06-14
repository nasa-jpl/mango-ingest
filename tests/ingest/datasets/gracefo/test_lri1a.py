import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.lri1a import GraceFOLri1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOLri1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOLri1ADataProduct
    expected_table_names = ['gracefo_lri1a_04_c', 'gracefo_lri1a_04_d']

    expected_field_types = [int, int, str,
        str, str, datetime,
        Union[float, type(None)], Union[int, type(None)], Union[int, type(None)], Union[int, type(None)],
        Union[int, type(None)], Union[int, type(None)], Union[int, type(None)], Union[int, type(None)],
        Union[int, type(None)], Union[int, type(None)], Union[int, type(None)], Union[int, type(None)]
    ]

    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849300, 82248034, 'C',
        '0000111111111111',  '00000000', datetime(2023, 5, 31, 23, 55, 0, 82248, tzinfo=timezone.utc),
        44343380776.80424,  1732163311,  2550604977, 1732163311,
        2551091333,  1732163311,  2551091268,  1732163311,
        2550604825,  4730,  17,  13),
        (738849300, 27372473, 'D',
        '0000111111111111',  '00000000', datetime(2023, 5, 31, 23, 55, 0, 27372, tzinfo=timezone.utc),
        46365245332.39905,  1811142395,  3422722281,  1811142395,
        3422061024,  1811142395,  3422060928,  1811142395,
        3422722192,  4702,  20,  124)
    ]
if __name__ == '__main__':
    unittest.main()
