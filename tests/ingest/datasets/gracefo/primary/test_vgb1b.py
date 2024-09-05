import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.vgb1b import GraceFOVgb1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOVgb1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOVgb1BDataProduct
    expected_table_names = ['gracefo_vgb1b_04_c', 'gracefo_vgb1b_04_d']
    expected_field_types = [int, str, float, float, float, float, str, datetime]
    expected_table_row_counts = [2, 2]
    expected_table_first_rows = [
        (580219200, 'C', 1.601125092708249, -0.975095579421092, -0.1873682458455261,
         -0.1186665557021665, '00000001', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (580219200, 'D', 1.601125092708249, -0.975095579421092, -0.1873682458455261,
         -0.1186665557021665, '00000001', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
