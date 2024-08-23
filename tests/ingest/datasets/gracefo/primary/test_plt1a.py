import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.plt1a import GraceFOPlt1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOPlt1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOPlt1ADataProduct
    expected_table_names = ['gracefo_plt1a_04_y']
    expected_field_types = [int, str, str,
                            float, float, float,
                            float, float, float,
                            float, str, datetime,
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849301, 'C', 'D',
         -0.000571730421932842, -4106820.187053568, 545180.4943109694,
         -5494199.497742502, 5975.636698578506, -1023.307817170906,
         -4581.752951524871,  '00000000', datetime(2023, 5, 31, 23, 55, 1, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
