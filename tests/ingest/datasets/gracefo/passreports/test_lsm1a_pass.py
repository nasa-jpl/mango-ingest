import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.lsm1a_pass import GraceFOLsm1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLsm1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOLsm1APassDataProduct
    expected_table_names = ['gracefo_lsm1a_pass_04_c', 'gracefo_lsm1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('LSM1A_2018-06-14_C_NYA_582254633.pass', 582206400, 607671656, 582254633.0515949,
            582258944.7263961, 41670, 0.1034744006630932, 0.01871696856738016,
            0.09999990463256836, 0.2049216032028198, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 6, 14, 0, 0, tzinfo=timezone.utc)),
        ('LSM1A_2018-06-14_D_NYA_582254630.pass', 582206400, 607671666, 582254630.6091238,
            582263319.1777021, 83970, 0.1034735268759096, 0.01870887787416283,
            0.09999990463256836, 0.2064938545227051, 8, 0, 0, 0, 0, 0, 0, 0,
            0, datetime(2018, 6, 14, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
