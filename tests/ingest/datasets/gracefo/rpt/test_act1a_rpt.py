import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.act1a_rpt import GraceFOAct1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOAct1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOAct1ARptDataProduct
    expected_table_names = ['gracefo_act1a_rpt_04_c', 'gracefo_act1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int, int, float, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('ACT1A_2023-06-01_C_04.dat', 738849600, 739543938, 738838700.084855, 738946899.985532,
         1082097, 0.09999103654119867, 7.713331636041786e-06, 0.09996688365936279,
         0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0, 0, 19, 1.899828,
         datetime(2023, 5, 31, 20, 58, 20, 84855, tzinfo=timezone.utc)),
        ('ACT1A_2023-06-01_D_04.dat', 738849600, 739546811, 738849322.589512, 738936299.984395,
         869853, 0.09999102707476284, 7.720259136813864e-06, 0.09996592998504639, 0.09999608993530273,
         8, 0, 0, 0, 0, 0, 0, 0, 0, 19, 1.899828, datetime(2023, 5, 31, 23, 55, 22, 589512, timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
