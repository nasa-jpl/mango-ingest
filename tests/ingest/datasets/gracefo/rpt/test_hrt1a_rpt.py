import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.hrt1a_rpt import GraceFOHrt1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOHrt1BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOHrt1ARptDataProduct
    expected_table_names = ['gracefo_hrt1a_rpt_04_c', 'gracefo_hrt1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('HRT1A_2023-06-01_C_04.dat', 738849600, 738951394, 738849609.506, 738935977.506,
         2700, 31.98815253609774, 0.4697297513132268, 9.50600004196167, 32,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 9, 506000, tzinfo=timezone.utc)),
        ('HRT1A_2023-06-01_D_04.dat', 738849600, 738956276, 738849623.506, 738935991.506,
         2700, 31.98815253609774, 0.4807670720507098, 8.49399995803833, 32.00199997425079,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 23, 506000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
