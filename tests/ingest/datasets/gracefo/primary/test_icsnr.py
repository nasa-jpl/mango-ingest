import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.icsnr import GraceFOIcsnrDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOIcsnrDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOIcsnrDataProduct
    expected_table_names = ['gracefo_icsnr_00_c', 'gracefo_icsnr_00_d']
    expected_field_types = [float, float, float, float, str, datetime]
    expected_table_row_counts = [200,200]
    expected_table_first_rows = [
        (739022409.0, -0.025890573859214783, 781.0, 767.0, '000',
         datetime(2023, 6, 3, 0, 0, 9, tzinfo=timezone.utc)),
        (739022409.0, -0.025527916848659515, 776.0, 760.0, '000',
         datetime(2023, 6, 3, 0, 0, 9, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
