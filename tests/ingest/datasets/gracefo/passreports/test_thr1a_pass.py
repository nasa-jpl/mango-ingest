import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.thr1a_pass import GraceFOThr1APassDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOThr1PassDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOThr1APassDataProduct
    expected_table_names = ['gracefo_thr1a_pass_04_c', 'gracefo_thr1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('THR1A_2018-06-01_C_NYA_581169229.pass', 581083200, 607670128, 581169229.883, 581180209.883,
         200, 55.17587939698493, 144.0523519473071, -0.2170000076293945, 1175.713000059128,
         8, 0, 0, 100, 100, 0, 0, 0, 0,
         datetime(2018, 6, 1, 23, 53, 49, 883000, tzinfo=timezone.utc)),
        ('THR1A_2018-06-01_D_NYA_581163591.pass', 581083200, 607670150, 581163591.883, 581174351.883,
         210, 51.48325358851675, 117.4981436493638, -0.2170000076293945, 814.2170000076294,
         8, 0, 0, 105, 105, 0, 0, 0, 0,
         datetime(2018, 6, 1, 22, 19, 51, 883000, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
