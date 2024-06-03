import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.gni1b import GraceFOGni1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOGni1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOGni1BDataProduct
    expected_table_names = ['gracefo_gni1b_04_c', 'gracefo_gni1b_04_d']
    expected_field_types = [int, str, str,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            float, float, float,
                            str, datetime
                            ]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 'C', 'I',
         -2130715.55343238, 215306.9570484998, -6542029.542409312,
         1e+33, 1e+33, 1e+33,
         7121.748887787898, -1163.082953597745, -2364.357584500985,
         1e+33, 1e+33, 1e+33,  '10000000'
         , datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
         (738849600, 'D', 'I',
          -2290675.693168949, 241402.7182871537, -6486678.549440263,
          1e+33, 1e+33, 1e+33,
          7061.134387187208, -1156.629164904302, -2543.563504581773,
          1e+33, 1e+33, 1e+33,  '10000000',
          datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
          )
    ]

    if __name__ == '__main__':
        unittest.main()
