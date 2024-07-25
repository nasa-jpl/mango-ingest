import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.primary.kbr1b import GraceFOKbr1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase
from typing import Union

class GraceFOKbr1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOKbr1BDataProduct
    expected_table_names = ['gracefo_kbr1b_04_y']

    expected_field_types = [int, float, float, float, float, float,
                            float, float, float, float, float,
                            float, float, float, float, str, datetime]

    expected_table_row_counts = [100]
    expected_table_first_rows = [
        (738849600, -404026.3387588505, -0.3205872106472599,
         0.0008595036893381769, -421420.5905655002, -0.000466571397198351,
         2.468128200422098e-07, 1.026739546166365e-10, 2.88885545250511,
         -8.69236941066078e-11, -3.336934846142346e-11, 783,
         769, 777, 762,  '00000000', datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]
if __name__ == '__main__':
    unittest.main()
