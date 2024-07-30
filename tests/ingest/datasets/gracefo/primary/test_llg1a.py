import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.llg1a import GraceFOLlg1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOLlg1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOLlg1ADataProduct
    expected_table_names = ['gracefo_llg1a_04_c', 'gracefo_llg1a_04_d']

    expected_field_types = [int, int, str, str, datetime]
    expected_table_row_counts = [26, 8]
    expected_table_first_rows = [
        (738851656, 1, 'C', 'Spbi: New top level state= SPB_STATE_PERSIST', datetime(2023, 6, 1, 0, 34, 16, tzinfo=timezone.utc)),
         (738878604, 1, 'D', 'Rmgr: Successful write to parameter file:', datetime(2023, 6, 1, 8, 3, 24, tzinfo=timezone.utc))
    ]

if __name__ == '__main__':
    unittest.main()
