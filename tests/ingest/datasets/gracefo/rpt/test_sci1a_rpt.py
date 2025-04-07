import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.sci1a_rpt import GraceFOSci1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOSci1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOSci1ARptDataProduct
    expected_table_names = ['gracefo_sci1a_rpt_04_c', 'gracefo_sci1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, float, float, float, float,
                            float, float, float, float, float,
                            float, float, float, float, float,
                            float, float, float, float, float,
                            datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('SCI1A_2023-06-01_C_04.dat', 738849600, 738951997, 738838761, 738946893,
         108133, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         100.00, 97.37, 71.27, 0.00, 0.00, 8.041033668270524, 7.216781072097587,
         136.3698353141939, 9.677214145989131, 7.482012962477555, 147.1260539705109,
         10.27107525955428, 8.380919222273006, 132.1950745591528, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('SCI1A_2023-06-01_D_04.dat', 738849600, 738956847, 738838761, 738946898,
         108138, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         100.00, 72.29, 98.49, 0.00, 0.00, 7.888136778873335, 7.146537587420103,
         119.3655827657932, 10.50864742896711, 7.855166573063811, 137.945414643199,
         8.901451080341067, 7.484851109094337, 127.7920409182275, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
