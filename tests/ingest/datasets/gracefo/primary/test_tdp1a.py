import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.tdp1a import GraceFOTdp1ADataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOTdp1ADatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOTdp1ADataProduct
    expected_table_names = ['gracefo_tdp1a_04_c', 'gracefo_tdp1a_04_d']
    expected_field_types = [int, float, float, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (741430800,  0.000000000000000e+00, -1.044695742597549e+01,  5.007258707185389e-03,
         '.Satellite.GRACEC.Clk.Bias', datetime(2023, 6, 30, 20, 59, 47, tzinfo=timezone.utc)),
        (741430800,  0.000000000000000e+00, -4.453536590253631e+00,
         4.930929621334951e-03, '.Satellite.GRACED.Clk.Bias', datetime(2023, 6, 30, 20, 59, 47, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
