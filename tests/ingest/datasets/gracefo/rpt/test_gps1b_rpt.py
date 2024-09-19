import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.gps1b_rpt import GraceFOGps1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOGps1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOGps1BRptDataProduct
    expected_table_names = ['gracefo_gps1b_rpt_04_c', 'gracefo_gps1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, int, float, int, float, int,
                            int, int, int, int, int, int,
                            datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('GPS1B_2023-06-01_C_04.dat', 738849600, 739543822, 738849600, 738935990,
         78286, 1.103531966532541, 3.133294889436077, 0, 10,
         8, 661, 661, 0, 0, 0, 0, 0, 0,
         0.002891922613473975, 78830, 0.003252601125266436, 78830, 0.004792565013324895,
         78830, 720, 0, 0, 0, 0, 989242,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('GPS1B_2023-06-01_D_04.dat', 738849600, 739543815, 738849600, 738935990,
         79001, 1.093544303797468, 3.120833845882666, 0, 10,
         8, 671, 671, 0, 0, 0, 0, 0, 0, 0.002866691930079107, 79556, 0.003274470033260115,
         79556, 0.004741169209567537, 79556, 719, 0, 0, 0, 0, 998813,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
