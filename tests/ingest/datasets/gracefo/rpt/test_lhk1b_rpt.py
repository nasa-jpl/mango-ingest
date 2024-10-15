import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.lhk1b_rpt import GraceFOLhk1BRptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFORbr1BRptDatasetReaderTestCase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOLhk1BRptDataProduct
    expected_table_names = ['gracefo_lhk1b_rpt_04_c', 'gracefo_lhk1b_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LHK1B_2023-06-01_C_04.dat', 738849600, 739547661, 738849600.1383209, 738935999.1783401,
         1169459, 0.07387955789703825, 0.2545325499682972, 0, 1.00406551361084,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 138321, tzinfo=timezone.utc)),
        ('LHK1B_2023-06-01_D_04.dat', 738849600, 739547664, 738849600.9464835, 738935998.9754002,
         1169424, 0.07388090444323105, 0.2543076501705909, 0, 1.003582835197449,
         8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 946483, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
