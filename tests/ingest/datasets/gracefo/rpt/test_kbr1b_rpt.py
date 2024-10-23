import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.kbr1b_rpt import GraceFOKbr1BRptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOKbr1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOKbr1BRptDataProduct
    expected_table_names = ['gracefo_kbr1b_rpt_04_y']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, float, float, int,
                            float, float, float, int, int,
                            float, float, float, float, float,
                            datetime]
    expected_table_row_counts = [1]
    expected_table_first_rows = [
        ('KBR1B_2023-06-01_Y_04.dat', 738849600, 739547414, 738849600,
         738935995, 17280, 5, 0, 5, 5, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         1.027642890886672e-06, 1.732010457197264e-06, 24, 17280, 0.1730097968333834,
         -0.5134711507707834, 0.5696822423487902, 1, 1441, -40.07945053892524,
         7.48874845792851, -66.28785714779539, -10.62262777740131 ,40.77307578498682,
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
