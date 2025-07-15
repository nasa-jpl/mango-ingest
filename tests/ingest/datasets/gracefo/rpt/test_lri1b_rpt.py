import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.lri1b_rpt import GraceFOLri1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOLri1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOLri1BRptDataProduct
    expected_table_names = ['gracefo_lri1b_rpt_04_y']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, float,  int,
                            float, float, float, int,
                            float, float, float, float,
                            datetime]
    expected_table_row_counts = [1]
    expected_table_first_rows = [
        ('LRI1B_2023-06-01_Y_04.dat', 738849600, 739548738, 738849600, 738935998, 43200, 2, 0, 2, 2,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         3.19153199856885e-09, 24, 8640, 0.0006027576977315756, -0.001152761280536652,
         0.00138169527053833, 1, 6.546861252735722e-08, 1.280205638079353e-10, 0, 0,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
