import unittest
from datetime import datetime, timezone
from typing import Union
from masschange.dataproducts.implementations.gracefo.primary.ihk1a import GraceFOIhk1ADataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOIhk1ADatasetReaderTestCase(DatasetReaderTestCaseBase):
    dataset_cls = GraceFOIhk1ADataProduct
    expected_table_names = ['gracefo_ihk1a_04_c', 'gracefo_ihk1a_04_d']
    expected_field_types = [int, int, str, str, str, float, str, Union[str, type(None)],
                            bool, bool, bool, bool, bool, bool, bool, bool, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849629, 0, 'C', '00000000', 'T', 24.42157524342042, '21', 'degC',
         False, False, False, False, False, False, False, False,
         datetime(2023, 6, 1, 0, 0, 29, tzinfo=timezone.utc)),
        (738849621, 0, 'D', '00000000', 'T', 25.63474238003979, '21', 'degC',
         False, False, False, False, False, False, False, False,
         datetime(2023, 6, 1, 0, 0, 21, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
