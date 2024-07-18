import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.lri1a import GraceFOLri1ADataProduct
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
        (738849600, 61552670, 'C',
         '0000111111111111', '00000000',  datetime(2023, 6, 1, 0, 0, 0, 61553, tzinfo=timezone.utc),
         47343173822.39327,  1849342727,  1877675238,  1849342727,
         1878163788,  1849342727,  1878163683,  1849342727,
         1877675390,  4782,  17,  14),
        (738849600, 531136, 'D',  '0000111111111111',  '00000000', datetime(2023, 6, 1, 0, 0, 0, 531, tzinfo=timezone.utc),
         49611169906.41313,  1937936324,  2015789850,  1937936324,
         2015146275,  1937936324 , 2015146317,  1937936324,
         2015789818,  4612,  20,  19)
    ]
if __name__ == '__main__':
    unittest.main()
