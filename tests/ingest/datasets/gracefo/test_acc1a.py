import unittest
from datetime import datetime, timezone
from masschange.ingest.datafilereaders.gracefoacc1a import GraceFOAcc1ADataFileReader
from masschange.datasets.gracefo.acc1a import GraceFOAcc1ADataset
from tests.ingest.ingesttestcase import IngestTestCase


class GraceFOAcc1ADatasetIngestTestCase(IngestTestCase):
    dataset_cls = GraceFOAcc1ADataset
    expected_table_names = ['gracefo_acc1a_c', 'gracefo_acc1a_d']
    expected_field_types = [float, float, float, float, float, float, int, datetime]
    expected_table_row_counts = [100, 100]
    expected_table_first_rows = [
        (-1.064995712362181e-05, -1.835037928999774e-07, -2.778613076425759e-07, 1.296553015708923e-05,
         0.0001192528009414673, -0.000781349539756775, 738849600007739,
         datetime(2023, 6, 1, 0, 0, 0, 7739, tzinfo=timezone.utc)),
        (-1.457772331862398e-05, 2.413741401248364e-06, -3.204377114688302e-06, 0.0001855893880128861,
         -0.00149625837802887, -0.0006985849142074586, 738849600011933,
         datetime(2023, 6, 1, 0, 0, 0, 11933, tzinfo=timezone.utc))
    ]

    if __name__ == '__main__':
        unittest.main()
