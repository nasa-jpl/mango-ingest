import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.tdp1b import GraceFOTdp1BDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOTdp1BDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOTdp1BDataProduct
    expected_table_names = ['gracefo_tdp1b_04_c', 'gracefo_tdp1b_04_d']
    expected_field_types = [float, float, float, float, str, datetime]
    expected_table_row_counts = [7, 7]
    expected_table_first_rows = [
        (775958800.0000,  0.00000000000000,  -110937.208992753,  0.0368,
         '.Satellite.GPS23.Clk.Bias', datetime(2024, 8, 3, 12, 6, 27, tzinfo=timezone.utc)),
        (775958800.0000,  0.00000000000000,  -110937.208992753,  0.0368,
         '.Satellite.GPS23.Clk.Bias', datetime(2024, 8, 3, 12, 6, 27, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
