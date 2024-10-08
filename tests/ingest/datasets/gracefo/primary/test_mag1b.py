import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.mag1b import GraceFOMag1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOMag1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOMag1BDataProduct
    expected_table_names = ['gracefo_mag1b_04_c', 'gracefo_mag1b_04_d']
    expected_field_types = [int, int, str, float, float, float, float, float, float, float, float,
                            float, float, float, float, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 83329, 'C', 12.32833957672119, 7.117527008056641, -31.84239959716797,
         4.621397018432617, 0, 1.34342896938324, 0, -2.094727039337158, 0.0, 0.0,
         65000.0, -65000.0, -137.4900054931641,  '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 83, tzinfo=timezone.utc)),
        (738849600, 82744, 'D', -12.77901935577393, -6.76025390625, -31.38417053222656,
         0.0, 0.0, 3.009282112121582, -4.619141101837158, -4.9951171875,
         0.0, 0.0, 65000.0, -65000.0, -137.4900054931641,  '00000000',
         datetime(2023, 6, 1, 0, 0, 0, 83, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
