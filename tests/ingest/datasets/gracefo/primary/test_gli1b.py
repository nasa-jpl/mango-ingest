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
        (741431100, 'C', 'E', 1766327.170161367, -4501220.723604153,
         4877491.423820307, 0.0009198433028575017, 0.001320906551201105,
         0.001057326205498364, 1828.799486826192, -5101.815149725497, -5347.889635895815,
         2.06388409305481e-06, 2.875120653425848e-06, 3.660564223037133e-06, '00000000',
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
