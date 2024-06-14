import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.gnv1a_prn import GraceFOGnv1APrnDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOGnv1APrnDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOGnv1APrnDataProduct
    expected_table_names = ['gracefo_gnv1a_prn_04_c', 'gracefo_gnv1a_prn_04_d']
    expected_field_types = [int, int, str,
                            float, float, float,
                            datetime]
    expected_table_row_counts = [1083, 1086]
    expected_table_first_rows = [
        (738849600, 10, 'C',
         3, 19.90209770202637, 20.45643615722656, datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc)),
         (738849600, 11, 'D',
         3, 18.44810485839844, 20.0234375, datetime(2023, 6, 1, 0, 0, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
