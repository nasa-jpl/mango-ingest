import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.lsm1b_rpt import GraceFOLsm1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFORbr1BRptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOLsm1BRptDataProduct
    expected_table_names = ['gracefo_lsm1b_rpt_04_c', 'gracefo_lsm1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LSM1B_2023-06-01_C_04.dat', 738849600, 739547645, 738849600.1011318, 738935999.9341846,
         834969, 0.1034768195341072, 1.976843593294361e-05, 0.1022356748580933, 0.1047227382659912,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 101132, tzinfo=timezone.utc)),
        ('LSM1B_2023-06-01_D_04.dat', 738849600, 739547763, 738849600.0490072, 738935999.8709809,
         834986, 0.103474699513981, 2.608382636645615e-05, 0.102696418762207, 0.1042617559432983,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 49007, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
