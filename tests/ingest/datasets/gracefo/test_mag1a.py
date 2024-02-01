import unittest
from datetime import datetime, timezone
from masschange.datasets.gracefo.mag1a import GraceFOMag1ADataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOMag1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOMag1ADataset
    expected_table_names = ['gracefo_mag1a_c', 'gracefo_mag1a_d']
    expected_field_types = [str, float, float, float, float, float, float, float, float,
                            float, float, float, float, float, str, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('C', 12.32833957672119, 7.117527008056641, -31.84239959716797,
        4.621397018432617, 0.0, 1.34342896938324, 0.0, -2.094727039337158, 0.0, 0.0,
        65000.0, -65000.0, -137.4900054931641,  '00000000', 738849600100000,
         datetime(2023, 6, 1, 0, 0, 0, 100000, tzinfo=timezone.utc)),
        ('D', -12.77901935577393, -6.76025390625, -31.38417053222656, 0.0, 0.0,
         3.009282112121582, -4.619141101837158, -4.9951171875, 0.0, 0.0,
         65000.0, -65000.0, -137.4900054931641,  '00000000', 738849600100000,
         datetime(2023, 6, 1, 0, 0, 0, 100000, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
