import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.tdp1b import GraceFOTdp1BDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOTdp1BDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOTdp1BDataProduct
    expected_table_names = ['gracefo_tdp1b_04_c', 'gracefo_tdp1b_04_d']
    expected_field_types = [int, float, float, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (741430800, 0.0, 58575.69968412829, 0.005007258707185389,
         '.Satellite.GRACEC.Clk.Bias', datetime(2023, 6, 30, 20, 59, 47, tzinfo=timezone.utc)),
        (741430800, 0.0, 60097.15141610575, 0.004930929621334951, '.Satellite.GRACED.Clk.Bias', datetime(2023, 6, 30, 20, 59, 47, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
