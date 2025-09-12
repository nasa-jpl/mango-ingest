import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.kbr1a_k_band import GraceFOKbr1AKBandDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOKbr1AKBandDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOKbr1AKBandDataProduct
    expected_table_names = ['gracefo_kbr1a_k_band_00_c', 'gracefo_kbr1a_k_band_00_d']
    expected_field_types = [float, float, datetime]
    expected_table_row_counts = [101,101]
    expected_table_first_rows = [
        (593467644.55,  -502585.7484802161,
         datetime(2018, 10, 22, 8, 7, 24, 550000, tzinfo=timezone.utc)),
        (593471399.55,  502526.3874813098,
         datetime(2018, 10, 22, 9, 9, 59, 550000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
