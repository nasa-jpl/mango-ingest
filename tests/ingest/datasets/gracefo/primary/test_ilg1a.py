import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.ilg1a import GraceFOIlg1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOIlg1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOIlg1ADataProduct
    expected_table_names = ['gracefo_ilg1a_04_c', 'gracefo_ilg1a_04_d']

    expected_field_types = [int, int, str, Union[str, type(None)], datetime]
    expected_table_row_counts = [72, 54]
    expected_table_first_rows = [
        (738849504, 1, 'C', 'OccAnt: @1369612704 FO:0 AO:0 FP:0 Tot:11', datetime(2023, 5, 31, 23, 58, 24, tzinfo=timezone.utc)),
        (738849501, 1, 'D', 'OccAnt: @1369612702 FO:0 AO:0 FP:0 Tot:11', datetime(2023, 5, 31, 23, 58, 21, tzinfo=timezone.utc))
    ]

if __name__ == '__main__':
    unittest.main()
