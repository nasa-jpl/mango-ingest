import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.clk1b import GraceFOClk1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOClk1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOClk1BDataProduct
    expected_table_names = ['gracefo_clk1b_04_c', 'gracefo_clk1b_04_d']
    expected_field_types = [int, str, int, float, float, float, float,
                            str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849300, 'C', -1, -0.01666656140839928, 1.599266764767988e-11,
         -1.536399985660976e-08, 1.653384827629116e-12,  '00000010',
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc)),
        (738849300, 'D', -1, -0.01725100262549603, 1.560976798810238e-11,
         -1.592273245780939e-08, 1.640211121486265e-12,  '00000010',
         datetime(2023, 5, 31, 23, 55, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
