import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.thr1a_pass import GraceFOThr1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOThr1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOThr1APassDataProduct
    expected_table_names = ['gracefo_thr1a_pass_04_c', 'gracefo_thr1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('THR1A_2018-05-22_C_NEN_580291484.pass', 580219200, 607670781, 580291484.881,
            580293772.88, 1578, 1.450855421657115, 2.073262411791452,
            -0.2200000286102295, 70.72000002861023, 8, 0, 0, 514, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc)),
        ('THR1A_2018-05-22_D_NEN_580291484.pass', 580219200, 607670752, 580291484.886,
            580300108.88, 2658, 3.245763643190831, 79.86341575865313,
            -0.2200000286102295, 4105.22000002861, 8, 0, 0, 899, 0, 0, 0, 0,
            0, datetime(2018, 5, 22, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
