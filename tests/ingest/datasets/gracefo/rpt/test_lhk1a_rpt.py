import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.lhk1a_rpt import GraceFOLhk1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFORbr1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):

    dataset_cls = GraceFOLhk1ARptDataProduct
    expected_table_names = ['gracefo_lhk1a_rpt_04_c', 'gracefo_lhk1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('LHK1A_2023-06-01_C_04.dat', 738849600, 738950149, 738849600.05267,
         738935999.0940167, 1169459, 0.07387955903219201, 0.2545325538909969,
         0, 1.00406551361084, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 52670, tzinfo=timezone.utc)),
        ('LHK1A_2023-06-01_D_04.dat', 738849600, 738955931, 738849600.0129863,
         738935999.0432788, 1169436, 0.07388100261451983, 0.2543078924058043,
         0, 1.003582954406738, 8, 0, 0, 0, 0, 0, 0, 0, 0,
         datetime(2023, 6, 1, 0, 0, 0, 12986, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
