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
        ('KBR1A_2018-05-29_C_NYA_580896884.pass', 580824000, 607670459, 580896884.55,
            580908164.45, 112800, 0.1000000000008455, 2.407068011038021e-08,
            0.09999990463256836, 0.1000000238418579, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 29, 0, 0, tzinfo=timezone.utc)),
        ('KBR1A_2018-05-29_D_NYA_580891304.pass', 580824000, 607670487, 580891304.55,
            580902524.45, 112200, 0.10000000000085, 1.21429607369055e-08,
            0.09999990463256836, 0.1000000238418579, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 29, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
