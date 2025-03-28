import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.kbr1a_pass import GraceFOKbr1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOKbr1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOKbr1APassDataProduct
    expected_table_names = ['gracefo_kbr1a_pass_04_c', 'gracefo_kbr1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('KBR1A_2018-06-01_C_NYA_581169224.pass', 581083200, 607670129, 581169224.55, 581180684.45, 114600,
         0.1000000000008322, 4.308310712210975e-08, 0.09999990463256836, 0.1000000238418579,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('KBR1A_2018-06-01_D_NYA_581163584.pass', 581083200, 607670151, 581163584.55, 581174984.45, 114000,
         0.1000000000008366, 3.789922213065588e-08, 0.09999990463256836, 0.1000000238418579,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2018, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
