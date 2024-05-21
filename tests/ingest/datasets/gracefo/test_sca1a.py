import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.sca1a import GraceFOSca1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOSca1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOSca1ADataProduct
    expected_table_names = ['gracefo_sca1a_04_c', 'gracefo_sca1a_04_d']
    expected_field_types = [int, int, str, int, str, float, float, float, float, int, int, int, str, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 29992, 'C', 1, 'P', -0.2800946131535094, -0.5818990067536707, 0.6977387079265291, 0.3100020129489717, 0, 47, 1, '11000001', '11000000',
         datetime(2023, 6, 1, 0, 0, 0, 29992, tzinfo=timezone.utc)),
        (738849600, 29988, 'D', 1, 'P', 0.07622107143425871, 0.7564510073877059, 0.6466095707487802, 0.06219553087423446, 0, 54, 1, '11000001', '11000000',
         datetime(2023, 6, 1, 0, 0, 0, 29988, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
