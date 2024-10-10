import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.mag1b_rpt import GraceFOMag1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1BRptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOMag1BRptDataProduct
    expected_table_names = ['gracefo_mag1b_rpt_04_c', 'gracefo_mag1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('MAG1B_2023-06-01_C_04.dat', 738849600, 739543739, 738849600.083329, 738935999.582001,
         172800, 0.4999999923148196, 5.866593028613784e-08, 0.499998927116394, 0.5,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 83329, tzinfo=timezone.utc)),
        ('MAG1B_2023-06-01_D_04.dat', 738849600, 739543741, 738849600.082744, 738935999.581369,
         172800, 0.4999999920430098, 5.960464477539062e-08, 0.499998927116394, 0.5,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 82744, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
