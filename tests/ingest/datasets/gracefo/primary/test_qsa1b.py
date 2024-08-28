import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.qsa1b import GraceFOQsa1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOQsa1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOQsa1BDataProduct
    expected_table_names = ['gracefo_qsa1b_04_c', 'gracefo_qsa1b_04_d']
    expected_field_types = [int, str,  int, float, float, float, float, float,
                            str, datetime]
    expected_table_row_counts = [3, 3]
    expected_table_first_rows = [
        (580219200, 'C', 1, -0.1846523033914243, 0.687786333034212, 0.676514247097443, 0.18756854858364,
         0, '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        (580219200, 'D', 1, -0.1789388979356683, 0.682734893544669, 0.68280707751296,
         0.188754949181018, 0, '00000000', datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
