import unittest
from datetime import datetime, timezone
from typing import Union
from masschange.dataproducts.implementations.gracefo.events.problemfiles_evnt import GraceFOProblemFilesEventsDataProduct
from tests.ingest.datasets.base import DatasetReaderTestCaseBase

class GraceFOProblemFilesEventsDatasetReaderTestCase(DatasetReaderTestCaseBase):

    test_data_path = './tests/input_data/events/'
    data_is_zipped = False
    maxDiff = None
    dataset_cls = GraceFOProblemFilesEventsDataProduct
    expected_table_names = ['gracefo_problemfiles_evnt_04_c', 'gracefo_problemfiles_evnt_04_d']
    expected_field_types = [str, str, float, Union[str, None], Union[str, None], str,
                            str, datetime]
    expected_table_row_counts = [104, 104]
    expected_table_first_rows = [
        ('comment', '2018-08-14 00:00:00', 587476800.0, None, None, 'C',
         '           Unregistered GF2_L0_ISP_NSCI_NSG_20180814T161352_RDC_D226.zip '
         + 'because it was assigned to GF2 wrongly and the same as a part of '
         + 'GF1_L0_ISP_NSCI_NYA_20180814T162150_RDC_D226.zip\n',
         datetime(2018, 8, 14, 0, 0, tzinfo=timezone.utc)),
        ('comment', '2018-08-14 00:00:00', 587476800.0, None, None, 'D',
         '           Unregistered GF2_L0_ISP_NSCI_NSG_20180814T161352_RDC_D226.zip '
         + 'because it was assigned to GF2 wrongly and the same as a part of '
         + 'GF1_L0_ISP_NSCI_NYA_20180814T162150_RDC_D226.zip\n',
         datetime(2018, 8, 14, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
