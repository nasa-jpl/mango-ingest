import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.icsnr import GraceFOIcsnrDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOIcsnrDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOIcsnrDataProduct
    expected_table_names = ['gracefo_icsnr_00_c', 'gracefo_icsnr_00_d']
    expected_field_types = [float, float, float, float, datetime]
    expected_table_row_counts = [101,101]
    expected_table_first_rows = [
        (738849609.0, 0.05265774577856064, 784.0, 774.0,
         datetime(2023, 6, 1, 0, 0, 9, tzinfo=timezone.utc)),
        (738849609.0, 0.05418538302183151, 780.0, 767.0,
         datetime(2023, 6, 1, 0, 0, 9, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
