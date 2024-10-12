import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.lhm1a_pass import GraceFOLhm1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOLhm1PassDatasetDatasetIngestTestCaseBase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOLhm1APassDataProduct
    expected_table_names = ['gracefo_lhm1a_pass_04_c', 'gracefo_lhm1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('LHM1A_2018-06-11_C_NYA_581975099.pass', 581947200, 607671349, -62769575.99750476, 581975459.2200191,
         96, 6786789.844394987, 65800331.71948773, 0, 644744675.2201263,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(1998, 1, 5, 0, 0, 24, 2495, tzinfo=timezone.utc)),
        ('LHM1A_2018-06-12_D_NYA_582060258.pass', 582033600, 607671434, -62769576.98709543, 582060618.2441159,
         96, 6787686.2655917, 65809022.84090935, 0, 644829835.1898309,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(1998, 1, 5, 0, 0, 23, 12905, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
