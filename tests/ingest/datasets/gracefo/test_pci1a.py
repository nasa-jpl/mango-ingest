import unittest
from datetime import datetime, timezone
from masschange.dataproducts.implementations.gracefo.pci1a import GraceFOPci1ADataProduct
from tests.ingest.datasets.base import DatasetIngestTestCaseBase


class GraceFOPci1ADatasetDatasetIngestTestCaseBase(DatasetIngestTestCaseBase):
    dataset_cls = GraceFOPci1ADataProduct
    expected_table_names = ['gracefo_pci1a_04_c', 'gracefo_pci1a_04_d']
    expected_field_types = [int, str, float, float, float, str, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (738849600, 'C', 1.444398040265439, -1.043760965730422e-10, -6.875266741762771e-12, '00000000',
         datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc)),
        (738849600, 'D', -1.444457412239671, -1.745240246643444e-11, 2.649408171966069e-11 , '00000000',
         datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
