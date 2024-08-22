import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.clk1a import GraceFOClk1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase

class GraceFOClk1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    test_data_path = './tests/input_data/test_unzipped/'
    data_is_zipped = False
    dataset_cls = GraceFOClk1ADataProduct
    expected_table_names = ['gracefo_clk1a_04_c', 'gracefo_clk1a_04_d']
    expected_field_types = [int, str, int, float, float, float, float,
                            str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738838700, 'C', -1, -0.01650371603572079, 4.584127827359897e-07,
         -1.536416656448646e-08, 7.451463477188681e-12,  '00000010',
         datetime(2023, 5, 31, 20, 58, 20, tzinfo=timezone.utc)),
        (738838700, 'D', -1, -0.01708221843967185, 4.584555612790305e-07,
         -1.592279949205651e-08, 7.275799953943833e-12,  '00000010',
         datetime(2023, 5, 31, 20, 58, 20, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
