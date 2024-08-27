import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.llk1b import GraceFOLlk1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOLlk1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):

    dataset_cls = GraceFOLlk1BDataProduct
    expected_table_names = ['gracefo_llk1b_04_c', 'gracefo_llk1b_04_d']
    expected_field_types = [int, str, int, float, float, float, float,
                            str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849300, 'C', -1, -0.01673896421047909, 1.123121185748755e-07,
         -1.536399985660976e-08, 1.653384827629116e-12, '00000010',
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc)),
        (738849300, 'D', -1, -0.01732340542757584, 1.123121185210052e-07,
         -1.592273245780939e-08, 1.640211121486265e-12,  '00000010',
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
