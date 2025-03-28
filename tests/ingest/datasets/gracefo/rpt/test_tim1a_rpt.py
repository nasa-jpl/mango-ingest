import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.tim1a_rpt import GraceFOTim1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOTim1BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOTim1ARptDataProduct
    expected_table_names = ['gracefo_tim1a_rpt_04_c', 'gracefo_tim1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('TIM1A_2010-12-23_C_02.dat', 346334400, 356564726, 346333800, 346421399,
         87600, 1, 0, 1, 1, 8, 0, 297, 0, 87600, 0, 0, 0, 0,
         datetime(2010, 12, 23, 0, 0, tzinfo=timezone.utc)),
        ('TIM1A_2010-12-23_D_02.dat', 346334400, 356564772, 346333800,
         346421399, 87600, 1, 0, 1, 1, 8, 0, 300, 0, 87600, 0, 0, 0, 0,
         datetime(2010, 12, 23, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
