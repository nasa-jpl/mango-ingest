import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.rpt.clk1a_rpt import GraceFOClk1ARptDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOClk1ARptDatasetReaderTestCase(DatasetReaderTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False

    dataset_cls = GraceFOClk1ARptDataProduct
    expected_table_names = ['gracefo_clk1a_rpt_04_c', 'gracefo_clk1a_rpt_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int,
                            int, int, int,
                            float, float, float, float, float, float, int,
                            float, float, float, float, float, float, int,
                            int, datetime]
    expected_table_row_counts = [1, 1]
    expected_table_first_rows = [
        ('CLK1A_2023-06-01_C_04.dat', 738849600, 738950326, 738838700, 738946900,
         10823, 9.998151912770282, 0.1359318096354548, 0, 10,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         -17334908.25156552, 458.4127827359897, -15.36415894584928,
         0.007451463477188681, 17283698.87306539, 20.02155886266438, 49138,
         -17334908.25156552, 458.4127827359897, -15.36415894584928,
         0.007451463477188681, 17283698.87306539, 20.02155886266438, 48881,
         0, datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        ('CLK1A_2023-06-01_D_04.dat', 738849600, 738956236, 738838700, 738946900,
         10823, 9.998151912770282 ,0.1359318096354548, 0, 10,
         8, 1, 1, 0, 0, 0, 0, 0, 0,
         -17943616.02113493, 458.4555612790305, -15.92279811767999,
         0.007275799953943832, 17947381.15347182, 25.54616809372829, 50515,
         -17943616.02113493, 458.4555612790305, -15.92279811767999,
         0.007275799953943832, 17947381.15347182, 25.54616809372829, 50437,
         0, datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
