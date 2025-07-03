import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.tim1a import GraceFOTim1ADataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOTim1ADatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOTim1ADataProduct
    expected_table_names = ['gracefo_tim1a_04_c', 'gracefo_tim1a_04_d']
    expected_field_types = [int, str, int, int, int, int, int,
                            str, bool, bool, bool, bool, bool, bool, bool, bool, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
         (346333800, 'C', 3, 0, 0, 19639, 19639, '00001000',
          False, False, False, False, True, False, False, False,
          datetime(2010, 12, 22, 23, 50, tzinfo=timezone.utc)),
         (346333800, 'D', 3, 0, 0, 6260, 6260, '00001000',
          False, False, False, False, True, False, False, False,
          datetime(2010, 12, 22, 23, 50, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
