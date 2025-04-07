import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.poe1a_rpt import GraceFOPoe1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOPoe1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOPoe1ARptDataProduct
    expected_table_names = ['gracefo_poe1a_rpt_04_c', 'gracefo_poe1a_rpt_04_d']
    expected_field_types = [str, float, float, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, int, int, float, float, int,
                            int, float, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('POE1A_2023-06-01_C_04.rpt', 738849600.0, 739457044.571405, 738838800.0, 738946800.0,
         95788, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         4.920343, 91649, 4139, 4.3, 55.09514, 91408, 4380, 4.6,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('POE1A_2023-06-01_D_04.rpt', 738849600.0, 739457055.737881, 738838800.0, 738946800.0,
         96473, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         5.005834, 92199, 4274, 4.4, 54.98781, 92427, 4046, 4.2,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
