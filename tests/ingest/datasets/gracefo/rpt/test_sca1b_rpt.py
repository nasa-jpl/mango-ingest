import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.sca1b_rpt import GraceFOSca1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFORSca1BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOSca1BRptDataProduct
    expected_table_names = ['gracefo_sca1b_rpt_04_c', 'gracefo_sca1b_rpt_04_d']
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
        ('SCA1B_2023-06-01_C_04.dat', 738849600, 739546487, 738849301, 738936300, 87000, 1, 0, 1, 1,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         100.00, 97.37, 71.27, 100.00, 0.00, 15.57200324671677, 9.526793777771964, 155.0404096234456,
         17.17704812860897, 11.53985729821876, 173.0801633028423, 15.8279097092616, 13.23793606148441,
         150.7728460739449, 0.3137789354456635, 0.2807018169273275, 0.2637422473369495, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 1, tzinfo=timezone.utc)),
        ('SCA1B_2023-06-01_D_04.dat', 738849600, 739545891, 738849301, 738936300, 87000, 1, 0, 1, 1,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         100.00, 72.29, 98.49, 100.00, 0.00, 12.46898300789607, 7.70316463553917, 138.9903250697234,
         17.40404666685315, 14.30873893730293, 161.0616592048385, 15.41679343225347, 14.15141286835234,
         153.857103472243, 0.3144137246312134, 0.28426566377949, 0.2861005263728063, 0, 0, 0,
         datetime(2023, 5, 31, 23, 55, 1, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
