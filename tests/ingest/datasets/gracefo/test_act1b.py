import unittest
from datetime import datetime, timezone
from masschange.datasets.implementations.gracefo.act1b import GraceFOAct1BDataset
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOAct1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOAct1BDataset
    expected_table_names = ['gracefo_act1b_c', 'gracefo_act1b_d']
    expected_field_types = [int, str, float, float, float, float, float, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 'C', -2.77938114592582e-07, -1.064874232988739e-05, -1.83231564779496e-07,
         -8.959451256558249e-11, 1.161562708673709e-09, 2.238855841952048e-10,  '00000000',
         datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc)),
        (738849600, 'D', 2.766468091592577e-07, 1.064957039541215e-05, -1.832523708702873e-07,
         3.760645125225072e-10, -3.938861161960659e-10, 6.198529328673559e-11,  '00000000',
         datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
