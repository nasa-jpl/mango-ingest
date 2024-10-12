import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.mag1a_pass import GraceFOMag1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOMag1PassDatasetDatasetIngestTestCaseBase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOMag1APassDataProduct
    expected_table_names = ['gracefo_mag1a_pass_04_c', 'gracefo_mag1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('MAG1A_2000-01-01_C_NYA_0.pass', -43200, 611323487, 0, 0, 91200, 0, 0, 0, 0,
         8, 0, 0, 68400, 0, 0, 0, 0, 0,
         datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc)),
        ('MAG1A_2000-01-01_D_NYA_0.pass', -43200, 611329008, 0, 0, 91680, 0, 0, 0, 0,
         8, 0, 0, 68760, 0, 0, 0, 0, 0,
         datetime(2000, 1, 1, 12, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
