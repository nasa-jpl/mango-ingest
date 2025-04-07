import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.lsm1a_rpt import GraceFOLsm1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLsm1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOLsm1ARptDataProduct
    expected_table_names = ['gracefo_lsm1a_rpt_04_c', 'gracefo_lsm1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LSM1A_2023-06-01_C_04.dat', 738849600, 738950144, 738849600.0154809,
         738935999.9533379, 834970, 0.1034768211239361, 1.97683902140016e-05,
         0.1022356748580933, 0.1047227382659912, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('LSM1A_2023-06-01_D_04.dat', 738849600, 738955926, 738849600.0120353,
         738935999.9388595, 834987, 0.1034747011617106, 2.608375916234637e-05,
         0.102696418762207, 0.1042617559432983, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
