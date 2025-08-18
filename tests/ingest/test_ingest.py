import os
import unittest
from datetime import datetime, timezone

from masschange.dataproducts.implementations.gracefo.primary.acc1a import GraceFOAcc1ADataProduct
from masschange.dataproducts.implementations.gracefo.offred.offred import GraceFOOffredDataProduct
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.ingest.executor.ingest import ingest_file_to_db
from tests.ingest.base import IngestTestCaseBase

class StubGraceFOOffredDataFileReader(GraceFOOffredDataFileReader):

    @classmethod
    def _get_current_input_file_column_def(cls, data_fpath: str):
        return super()._get_current_input_file_column_def(data_fpath, check_time_col_names=False)

class StabGraceFOOffredDataProduct(GraceFOOffredDataProduct):
    @classmethod
    def get_reader(cls):
        return StubGraceFOOffredDataFileReader()

class DataOverwriteIngestTestCase(IngestTestCaseBase):
    """Test behaviour related to deleting old data when new overlapping data is ingested"""
    ingest_repetitions = 3
    input_dir = './tests/input_data/ingest/test_ingest/'
    expected_record_count = 20  # ten from each of two files

    product1 = GraceFOAcc1ADataProduct()
    product2 = StabGraceFOOffredDataProduct()
    version = DatasetVersion('04')
    instrument_id = 'C'


    @classmethod
    def _get_current_input_file_column_def(cls, data_fpath: str):
        return super()._get_current_input_file_column_def(data_fpath, check_time_col_names=False)

    def setUp(self):
        self.dataset1 = DatasetFactory.create(self.product1, self.version, self.instrument_id)
        self.dataset2 = DatasetFactory.create(self.product2, DatasetVersion('01'), 'GF1')

        self.input_filepaths = [os.path.join(self.input_dir, fn) for fn in os.listdir(self.input_dir)]
        super().__init__()

    def test_repeated_ingestion_does_not_accumulate_data(self):

        previous_record_count = None

        for _ in range(self.ingest_repetitions):
            for fp in self.input_filepaths:
                ingest_file_to_db(self.product1, fp)

            current_record_count = len(self.dataset1.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                           datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                           aggregation_level=0,
                                                           limit_data_span=False))
            if previous_record_count is None:
                previous_record_count = current_record_count

            self.assertEqual(previous_record_count, current_record_count)

            previous_record_count = current_record_count

    def test_duplication_handling_base_on_source_file_name(self):

        # test that repeated ingestion of the same file does not increase data count
        previous_record_count = None
        data_file1 = './tests/input_data/offred/GF1_CX_777777_XXX_7_777777777_7777_77777777777_77777777777.out'
        os.environ['OFFREAD_METADATA_FILE'] = './tests/input_data/offred/fake_fields_metadata.json'
        for _ in range(self.ingest_repetitions):
            ingest_file_to_db(self.product2, data_file1)
            current_record_count = len(self.dataset2.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                           datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                           aggregation_level=0,
                                                           limit_data_span=False))
            if previous_record_count is None:
                previous_record_count = current_record_count

            self.assertEqual(previous_record_count, current_record_count)

            previous_record_count = current_record_count

        # Test that ingestion of a file with a different file name, but the same content will increase the count 2 times
        data_file2 = './tests/input_data/GF1_CX_888888_XXX_7_777777777_7777_77777777777_77777777777.out'
        for _ in range(self.ingest_repetitions):
            ingest_file_to_db(self.product2, data_file2)

        current_record_count = len(self.dataset2.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                        datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                        aggregation_level=0,
                                                        limit_data_span=False))
        self.assertEqual(previous_record_count * 2, current_record_count)

    def test_all_expected_data_present(self):

        for _ in range(self.ingest_repetitions):
            for fp in self.input_filepaths:
                ingest_file_to_db(self.product1, fp)

            record_count = len(self.dataset1.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                   datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                   aggregation_level=0,
                                                   limit_data_span=False))

            self.assertEqual(self.expected_record_count, record_count)


if __name__ == '__main__':
    unittest.main()
