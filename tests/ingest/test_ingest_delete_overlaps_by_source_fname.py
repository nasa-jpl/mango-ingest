import os
import unittest
from tests.ingest.base import IngestTestCaseBase
from datetime import datetime, timezone, timedelta

from masschange.dataproducts.implementations.gracefo.offred.offred import GraceFOOffredDataProduct
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.ingest.executor.ingest import ingest_file_to_db
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.datasetfactory import DatasetFactory

class StubGraceFOOffredDataFileReader(GraceFOOffredDataFileReader):

    @classmethod
    def _get_current_input_file_column_def(cls, data_fpath: str):
        return super()._get_current_input_file_column_def(data_fpath, check_time_col_names=False)

    @classmethod
    def validate_timestamp(cls, filepath: str, timestamps) -> bool:
            #OFFRED unit tests uses mock input data, so skip the check for timestamp outliers
            return True

class StabGraceFOOffredDataProduct(GraceFOOffredDataProduct):
    @classmethod
    def get_reader(cls):
        return StubGraceFOOffredDataFileReader()

class DataOverwriteByFileNameIngestTestCase(IngestTestCaseBase):
    """Test behaviour related to deleting old data based on source file name when new overlapping data is ingested"""
    ingest_repetitions = 3
    product = StabGraceFOOffredDataProduct()
    version = DatasetVersion('04')
    data_file1 = './tests/input_data/offred_unzipped/GF1_CX_777777_YYY_4_777777777_7777_77777777777_77777777777.out'
    data_file2 = './tests/input_data/offred_unzipped/GF1_CX_777777_ZZZ_4_777777777_7777_77777777777_77777777777.out'
    expected_record_count = 132 # 6 lines, 11 variables, 2 files
    def setUp(self):
        super().setUp()
        self.dataset = DatasetFactory.create(self.product, DatasetVersion('00'), 'GF1' )
        os.environ['OFFRED_METADATA_FILEPATH'] = './tests/input_data/offred/fake_fields_metadata.json'

    def test_repeated_ingestion_does_not_accumulate_data(self):
        previous_record_count = None

        for _ in range(self.ingest_repetitions):
            ingest_file_to_db(self.product, self.data_file1)
            current_record_count = len(self.dataset.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                           datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                           aggregation_level=0,
                                                           limit_data_span=False))
            if previous_record_count is None:
                previous_record_count = current_record_count

            self.assertEqual(previous_record_count, current_record_count)

            previous_record_count = current_record_count

    def test_ingestion_same_data_from_different_files_accumulates_data(self):

        # Test that ingestion of a file with a different file name, but the same content will increase the count 2 times

        # ingest from file1
        for _ in range(self.ingest_repetitions):
            ingest_file_to_db(self.product, self.data_file1)

        previous_record_count = len(self.dataset.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                       datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                       aggregation_level=0,
                                                       limit_data_span=False))
        # ingest from file 2 (same data, but different file name)
        for _ in range(self.ingest_repetitions):
            ingest_file_to_db(self.product, self.data_file2)

        current_record_count = len(self.dataset.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                        datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                        aggregation_level=0,
                                                        limit_data_span=False))

        self.assertEqual(previous_record_count * 2, current_record_count)
        self.assertEqual(self.expected_record_count, current_record_count)

if __name__ == '__main__':
    unittest.main()
