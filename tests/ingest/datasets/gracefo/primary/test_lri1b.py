import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.lri1b import GraceFOLri1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOLri1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOLri1BDataProduct
    expected_table_names = ['gracefo_lri1b_04_y']

    expected_field_types = [int, float, float, float, float, float, float, float, float, float, str, datetime]

    expected_table_row_counts = [100]
    expected_table_first_rows = [
        (738849600, 28053.39178652767, -0.3205874184807668, 0.0008595180149075108, 6.546861252735722e-08,
        -0.0005137081374612422, 4.925851942859082e-07, 4.446151285610963e-10, 88, 87, '00000000',
        datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
