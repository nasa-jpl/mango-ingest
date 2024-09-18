import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.sca1a_rpt import GraceFOSca1ARptDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFORbr1RptDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOSca1ARptDataProduct
    expected_table_names = ['gracefo_sca1a_rpt_04_c', 'gracefo_sca1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,  datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('SCA1A_2023-06-01_C_04.dat', 738849600, 738950157, 738838700.029985,
         738946899.529985, 649200, 0.1666661532134215, 0.2297149445838784,
         0, 0.5000450611114502, 8, 67644, 0, 0, 0, 0, 0, 643086, 581621,
         datetime(2023, 5, 31, 20, 58, 20, 29985, tzinfo=timezone.utc)),
        ('SCA1A_2023-06-01_D_04.dat', 738849600, 738955938, 738838700.029987,
         738946899.52999, 649200, 0.1666661532180121, 0.2312521789367916,
         0, 0.5000299215316772, 8, 63219, 0, 0, 0, 0, 0, 646057, 585976,
         datetime(2023, 5, 31, 20, 58, 20, 29987, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
