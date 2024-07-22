import unittest
from masschange.ingest import ingest
import os
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.pass_.acc1a_pass import GraceFOAcc1APassDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOAcc1PassDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    @classmethod
    def setUpClass(cls):
        super(DatasetIngestTestCaseBase, cls).setUpClass()
        ingest.run(product=cls.dataset_cls(), src=os.path.abspath('./tests/input_data/test_pass_reports'),
                   data_is_zipped=False)

    dataset_cls = GraceFOAcc1APassDataProduct
    expected_table_names = ['gracefo_acc1a_pass_04_c', 'gracefo_acc1a_pass_04_d']
    expected_field_types = [str, int, int, float, float,
                            int, float, float, float, float,
                            int, int, int, int, int, int, int, int,
                            int, int, int, int, int, int, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('ACC1A_2018-06-01_C_NYA_581169227.pass', 581083200, 607670127, 581169227.053822,
            581180686.927834, 114610, 0.09999104792811567, 7.670591756353221e-06,
            0.09996688365936279, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0,
            114610, 0, 0, 0, 0, 0, 0, 0, datetime(2018, 6, 1, 23, 53, 47, 53822, tzinfo=timezone.utc)),
        ('ACC1A_2018-06-01_D_NYA_581163588.pass', 581083200, 607670148, 581163588.056635,
            581174987.961355, 114010, 0.09999127016243252, 7.00742367078875e-06,
            0.09996891021728516, 0.09999406337738037, 8, 0, 0, 0, 0, 0, 0, 0, 114010, 0, 0, 0, 0, 0, 0, 0,
            datetime(2018, 6, 1, 22, 19, 48, 56635, timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
