import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.sca1b import GraceFOSca1BDataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOSca1BDatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOSca1BDataProduct
    expected_table_names = ['gracefo_sca1b_04_c', 'gracefo_sca1b_04_d']
    expected_field_types = [int, str, int, float, float, float, float, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 'C', 23, 0.07816222822585139, 0.1642723922577685, -0.004109730449778668,
         0.9833048140804912, 8.773305180563576e-07, '00000000',
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc)),
        (738849600, 'D', 23, 0.983305646617685, 0.003743257441020539, 0.1642246520986546,
         -0.07827041016800819, 8.773714539288127e-07, '00000000',
         datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc))
    ]

if __name__ == '__main__':
    unittest.main()
