import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.gni1b_rpt import GraceFOGni1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOGni1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOGni1BRptDataProduct
    expected_table_names = ['gracefo_gni1b_rpt_04_c', 'gracefo_gni1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            int, float, float, float, float, float, float,
                            int, float, float, float, float, float, float,
                            datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('GNI1B_2023-06-01_C_04.dat', 738849600, 739543737, 738849540, 738936060,
         86521, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 86521,
         14401, 0.002017607787947164, 0.009899781777730378, 0.004270933088610364,
         4.862720043206849e-06, 1.13550384821924e-05, 3.47069643373991e-06,
         14401, 0.001787760827695248, 0.005463053187541627, 0.002534971383368764,
         3.701654105880237e-06, 6.604835477103805e-06, 3.444007003889028e-06,
         datetime(2023, 5, 31, 23, 59, tzinfo=timezone.utc)),
        ('GNI1B_2023-06-01_D_04.dat', 738849600, 739544037, 738849540, 738936060,
         86521, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 86521,
         14401, 0.001741446368479414, 0.01007938145864921, 0.004140749907957171,
         4.810517301041263e-06, 1.157779674947075e-05, 2.833174480021898e-06,
         14401, 0.001680746709101903, 0.005419900502299671, 0.00224723976196803,
         3.344332875715277e-06, 6.494173616179761e-06, 3.327234025344341e-06,
         datetime(2023, 5, 31, 23, 59, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
