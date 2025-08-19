import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.ac01b_rpt import GraceFOAc01BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOAc01BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOAc01BRptDataProduct
    expected_table_names = ['gracefo_ac01b_rpt_04_c', 'gracefo_ac01b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int, int, float, float, float,
                            float, float, float, float, float, float,
                            float, float, float, float, float, float,
                            datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('AC01B_2023-07-01_C_04.dat', 741441600, 774498711, 741441600, 741527999, 86400,
         1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         1.719144466450516e-08, 1.449262470495271e-08 ,4.937085401708539e-10,
         0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc)),
        ('AC01B_2023-07-01_D_04.dat', 741441600, 774499066, 741441600, 741527999, 86400,
         1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        2.952470643682145e-08, 2.163184090705202e-08 ,1.126315088867143e-09,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, datetime(2023, 7, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
