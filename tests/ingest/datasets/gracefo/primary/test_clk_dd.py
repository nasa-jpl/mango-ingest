import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.clk_dd import GraceFOClkDdDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOClkDdDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOClkDdDataProduct
    expected_table_names = ['gracefo_clk_dd_04_y']
    expected_field_types = [int, float, float, float, datetime]
    expected_table_row_counts = [101]
    expected_table_first_rows = [
        (738842400, -6.182543019206399e-11, -1.960438061887615e-11, -4.222104957318784e-11,
         datetime(2023, 5, 31, 22, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
