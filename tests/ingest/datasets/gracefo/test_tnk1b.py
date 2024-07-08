import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.tnk1b import GraceFOTnk1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOTnk1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOTnk1BDataProduct
    expected_table_names = ['gracefo_tnk1b_04_c', 'gracefo_tnk1b_04_d']

    expected_field_types = [int, int, str, int, str, str, datetime,
                            Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)], Union[float, type(None)],
                            Union[float, type(None)], Union[float, type(None)]
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 364329, 'C', 1, '00000000', '00000011' ,datetime(2023, 6, 1, 0, 0, 0, 364329, tzinfo=timezone.utc),
         221.8955993652344, 1.507822036743164, None, None, None, None, None),
        (738849600, 363744, 'D', 1, '00000000', '00000011',  datetime(2023, 6, 1, 0, 0, 0, 363744, tzinfo=timezone.utc),
         224.9555053710938, 1.602975010871887, None, None, None, None, None)
    ]
if __name__ == '__main__':
    unittest.main()
