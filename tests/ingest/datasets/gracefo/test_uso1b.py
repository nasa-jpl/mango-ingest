import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.uso1b import GraceFOUso1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOUso1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOUso1BDataProduct
    expected_table_names = ['gracefo_uso1b_04_c', 'gracefo_uso1b_04_d']

    expected_field_types = [int, str, int, float,  float, float, str, datetime]

    expected_table_row_counts = [2, 2]
    expected_table_first_rows = [
        (738849300, 'C', -1, 4832000.074238848, 24527232376.83639, 32702976502.44852,
         '00000010' ,datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc)),
        (738849300, 'D', -1, 4832099.076940221, 24527734914.54856, 32703646552.73141,
         '00000010',  datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
