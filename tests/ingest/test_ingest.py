import os
import unittest
from datetime import datetime

from masschange.datasets.implementations.gracefo.acc1a import GraceFOAcc1ADataset
from masschange.ingest.ingest import ingest_file_to_db
from tests.ingest.datasets.base import IngestTestCaseBase


class DataOverwriteIngestTestCase(IngestTestCaseBase):
    """Test behaviour related to deleting old data when new overlapping data is ingested"""
    ingest_repetitions = 3
    input_dir = './tests/input_data/ingest/test_ingest/'
    expected_record_count = 20  # ten from each of two files

    def setUp(self):
        self.input_filepaths = [os.path.join(self.input_dir, fn) for fn in os.listdir(self.input_dir)]
        super().__init__()

    def test_repeated_ingestion_does_not_accumulate_data(self):
        dataset = GraceFOAcc1ADataset()
        previous_record_count = None

        for _ in range(self.ingest_repetitions):
            for fp in self.input_filepaths:
                ingest_file_to_db(dataset, fp)

            current_record_count = len(dataset.select('C', datetime(2000, 1, 1), datetime(2999, 1, 1), limit_data_span=False))
            if previous_record_count is None:
                previous_record_count = current_record_count

            self.assertEqual(previous_record_count, current_record_count)

            previous_record_count = current_record_count

    def test_all_expected_data_present(self):
        dataset = GraceFOAcc1ADataset()

        for _ in range(self.ingest_repetitions):
            for fp in self.input_filepaths:
                ingest_file_to_db(dataset, fp)

            record_count = len(dataset.select('C', datetime(2000, 1, 1), datetime(2999, 1, 1), limit_data_span=False))

            self.assertEqual(self.expected_record_count, record_count)


if __name__ == '__main__':
    unittest.main()
