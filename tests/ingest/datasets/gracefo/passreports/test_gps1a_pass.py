import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.gps1a_pass import GraceFOGps1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOGps1PassDatasetDatasetIngestTestCaseBase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOGps1APassDataProduct
    expected_table_names = ['gracefo_gps1a_pass_04_c', 'gracefo_gps1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('GPS1A_2018-06-01_C_NYA_581169225.pass', 581083200, 607670129, 581169225, 581180684,
         102890, 0.1113724499217603, 0.3145927960398733, 0, 1,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
          datetime(2018, 6, 1, 23, 53, 45, tzinfo=timezone.utc)),
        ('GPS1A_2018-06-01_D_NYA_581163585.pass', 581083200, 607670151, 581163585, 581174984,
         104600, 0.1089780973049456, 0.311611732148742, 0, 1,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 22, 19, 45, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
