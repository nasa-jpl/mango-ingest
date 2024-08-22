import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.llt1a import GraceFOLlt1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOLlt1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOLlt1ADataProduct
    expected_table_names = ['gracefo_llt1a_04_y']
    expected_field_types = [int, str, str,
                            float, float, float,
                            float, float, float,
                            float, str, datetime,
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738847800, 'C', 'D',
         -0.0005742609320594731, -4987098.972337563, 868444.8157530363,
         4619817.44117552, -5111.944931020933, 700.7618615131532,
         -5623.492077053641,  '00000000', datetime(2023, 5, 31, 23, 30, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
