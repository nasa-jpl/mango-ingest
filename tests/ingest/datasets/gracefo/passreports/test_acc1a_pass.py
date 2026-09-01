import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.acc1a_pass import GraceFOAcc1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAcc1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOAcc1APassDataProduct
    expected_table_names = ['gracefo_acc1a_pass_04_c', 'gracefo_acc1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int,
                            int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('ACC1A_2018-05-28_C_WHM_580778507.pass', 580737600, 607670503, 580778507.081407,
            580801461.938679, 229570, 0.09999110189977271, 7.510137428111266e-06,
            0.09996688365936279, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0,
            229570, datetime(2018, 5, 28, 0, 0, tzinfo=timezone.utc)),
        ('ACC1A_2018-05-28_D_WHM_580783728.pass', 580737600, 607670527, 580783728.107536,
            580806030.949006, 223049, 0.09999121924430225, 7.164588358187394e-06,
            0.09996795654296875, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0,
            223049, datetime(2018, 5, 28, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
