import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.acc1a import GraceFOAcc1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOAcc1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOAcc1ADataProduct
    expected_table_names = ['gracefo_acc1a_04_c', 'gracefo_acc1a_04_d']
    expected_field_types = [int, int, str, str, float, float, float, float, float, float, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 7739, 'C', '00000000', -1.064995712362181e-05, -1.835037928999774e-07, -2.778613076425759e-07, 1.296553015708923e-05,
         0.0001192528009414673, -0.000781349539756775, 0,
         datetime(2023, 6, 1, 0, 0, 0, 7739, tzinfo=timezone.utc)),
        (738849600, 11933, 'D', '00000000', -1.457772331862398e-05, 2.413741401248364e-06, -3.204377114688302e-06, 0.0001855893880128861,
         -0.00149625837802887, -0.0006985849142074586, 0,
         datetime(2023, 6, 1, 0, 0, 0, 11933, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
