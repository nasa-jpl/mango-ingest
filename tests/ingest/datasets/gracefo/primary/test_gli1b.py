import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.gli1b import GraceFOGli1BDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOGli1BDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOGli1BDataProduct
    expected_table_names = ['gracefo_gli1b_04_c', 'gracefo_gli1b_04_d']
    expected_field_types = [int, str, str,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            str, bool, bool, bool, bool, bool, bool, bool, bool, datetime
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (741431100, 'C', 'I', -4680115.866651726, 1172142.136565429, 4888089.662690016, 1e+33, 1e+33, 1e+33,
        -5314.256188747514, 1127.236712383796, -5335.877549112582, 1e+33, 1e+33, 1e+33, '10000000',
         True, False, False, False, False, False, False, False,
         datetime(2023, 6, 30, 21, 5, 0, 0, tzinfo=timezone.utc)),
         (741431100, 'D', 'I',
          -4559429.182158524, 1146380.810912626,5006323.734105522,
          1e+33, 1e+33, 1e+33,
          -5441.594769196445, 1159.056319801532, -5199.161794824314,
          1e+33, 1e+33, 1e+33,  '10000000',
          True, False, False, False, False, False, False, False,
          datetime(2023, 6, 30, 21, 5, 0, 0, tzinfo=timezone.utc)
          )
    ]
if __name__ == '__main__':
    unittest.main()
