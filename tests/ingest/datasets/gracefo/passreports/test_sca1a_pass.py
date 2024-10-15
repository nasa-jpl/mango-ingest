import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.passreports.sca1a_pass import GraceFOSca1APassDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase


class GraceFOSca1PassDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/test_passreports/'
    data_is_zipped = False

    dataset_cls = GraceFOSca1APassDataProduct
    expected_table_names = ['gracefo_sca1a_pass_04_c', 'gracefo_sca1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('SCA1A_2018-06-01_C_NYA_581169227.pass', 581083200, 607670128, 581169227.529977, 581180687.029986,
         68760, 0.1666618189482091, 0.2329590404936838, -0.1250150203704834, 0.6250170469284058,
         8, 4258, 0, 0, 0, 0, 0, 68035, 64495,
         datetime(2018, 6, 1, 23, 53, 47, 529977, tzinfo=timezone.utc)),
        ('SCA1A_2018-06-01_D_NYA_581163588.pass', 581083200, 607670150, 581163588.52999, 581174988.029996,
         68400, 0.1666617933899571, 0.2325204847241122, -0.1250150203704834, 0.6250170469284058,
         8, 4057, 0, 0, 0, 0, 0, 68142, 64343,
         datetime(2018, 6, 1, 22, 19, 48, 529990, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
