import unittest
from datetime import datetime, timezone

from masschange.datasets.gracefo.act1a import GraceFOAct1ADataset
from tests.ingest.ingesttestcase import IngestTestCase


class GraceFOAct1ADatasetIngestTestCase(IngestTestCase):
    dataset_cls = GraceFOAct1ADataset
    expected_table_names = ['gracefo_act1a_c', 'gracefo_act1a_d']
    expected_field_types = [str, str, float, float, float, float, float, float, int, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        ('C', '00000000', -1.064995712362181e-05, -1.835037928999774e-07, -2.778613076425759e-07, 0.0, 0.0, 0.0, 0, 738849600007739,
         datetime(2023, 6, 1, 0, 0, 0, 7739, tzinfo=timezone.utc)),
        ('D', '00000000',1.065002862882089e-05, -1.834087143007803e-07, 2.772959238815301e-07, 0.0, 0.0, 0.0, 20841, 738849600064528,
         datetime(2023, 6, 1, 0, 0, 0, 64528, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
