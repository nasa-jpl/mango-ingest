import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.gnv1b_rpt import GraceFOGnv1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOGnv1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOGnv1BRptDataProduct
    expected_table_names = ['gracefo_gnv1b_rpt_04_c', 'gracefo_gnv1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            int, float, float, float, float, float, float,
                            int, float, float, float, float, float, float,
                            datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('GNV1B_2023-06-01_C_04.dat', 738849600, 739543761, 738849540, 738936060,
         86521, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         14401, 0.002017607746864661, 0.00239158571857368, 0.001664940512546671,
         2.355188695607104e-06, 3.415220119598389e-06, 3.386988134000705e-06,
         14401, 0.001787760811143607, 0.001631619768302269, 0.001882265474899576,
         3.227701368569237e-06, 2.638845271432339e-06, 3.495725683457853e-06,
         datetime(2023, 5, 31, 23, 59, tzinfo=timezone.utc)),
        ('GNV1B_2023-06-01_D_04.dat', 738849600, 739544004, 738849540, 738936060,
         86521, 1, 0, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         14401, 0.001741446347300995, 0.001850738641451475, 0.001331598855705912,
         2.277951611621734e-06, 2.974062562902328e-06, 2.764528325005779e-06,
         14401, 0.001680746698755903, 0.001369182653390492 ,0.001856238666953796,
         3.029341829964894e-06, 2.343628426815486e-06, 3.361669380599001e-06,
         datetime(2023, 5, 31, 23, 59, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
