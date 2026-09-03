import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.tnk1a_pass import GraceFOTnk1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOTnk1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOTnk1APassDataProduct
    expected_table_names = ['gracefo_tnk1a_pass_04_c', 'gracefo_tnk1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('TNK1A_2018-05-22_C_NYA_580300953.pass', 580219200, 607673388, 580300953.381,
            580312289.506, 7088, 1.599566106956399, 1.934633050050803,
            0.0, 4.001999974250793, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        ('TNK1A_2018-05-22_D_NYA_580282624.pass', 580219200, 607673515, 580282624.3789999,
            580306588.381, 14980, 1.599839909212462, 1.934678132011119,
            0.0, 4.001000046730042, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
