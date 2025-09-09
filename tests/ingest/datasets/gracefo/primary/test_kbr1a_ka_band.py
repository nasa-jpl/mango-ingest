import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.kbr1a_ka_band import GraceFOKbr1AKaBandDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOKbr1AKaDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOKbr1AKaBandDataProduct
    expected_table_names = ['gracefo_kbr1a_ka_band_00_c', 'gracefo_kbr1a_ka_band_00_d']
    expected_field_types = [float, float, datetime]
    expected_table_row_counts = [101,101]
    expected_table_first_rows = [
        (593467594.55,  -670115.0112301861,
         datetime(2018, 10, 22, 8, 6, 34, 550000, tzinfo=timezone.utc)),
        (593471449.55,  670033.4432413918,
         datetime(2018, 10, 22, 9, 10, 49, 550000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
