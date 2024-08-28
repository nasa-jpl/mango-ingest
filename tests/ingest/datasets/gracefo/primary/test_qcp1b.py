import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.qcp1b import GraceFOQcp1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOQcp1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOQcp1BDataProduct
    expected_table_names = ['gracefo_qcp1b_04_c', 'gracefo_qcp1b_04_d']
    expected_field_types = [int, str,  float, float, float, float, float,
                            str, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        (580219200, 'C', 0.9999999696390049, 2.0838998e-08, -0.000228999977246, -9.0999996484e-05, 0,
         '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (580219200, 'D', 0.999999909150795, -2.3871235e-08, -5.6499974515e-05, 0.000422499886197, 0,
         '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
