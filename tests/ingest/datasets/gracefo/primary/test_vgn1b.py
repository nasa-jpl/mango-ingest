import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.vgn1b import GraceFOVgn1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOVgn1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOVgn1BDataProduct
    expected_table_names = ['gracefo_vgn1b_04_c', 'gracefo_vgn1b_04_d']
    expected_field_types = [int, str, float, float, float, float, str, datetime]
    expected_table_row_counts = [2, 2]
    expected_table_first_rows = [
        (580219200, 'C', 0.551508, 0.471862, -0.002326, -0.881669,
         '00000001', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (580219200, 'D', 0.550719, 0.472191, -0.001959, -0.881494,
         '00000001', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
