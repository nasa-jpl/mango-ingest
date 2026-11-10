import os
import unittest
from datetime import datetime, timezone
import glob

from masschange.dataproducts.implementations.gracefo.offred.offred import GraceFOOffredDataProduct
from masschange.dataproducts.implementations.gracefo.primary.acc1a import GraceFOAcc1ADataProduct

from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.ingest.executor.errors import TimestampOutliersException
from masschange.ingest.executor.ingest import ingest_file_to_db
from tests.ingest.base import IngestTestCaseBase


class DataOverwriteIngestTestCase(IngestTestCaseBase):
    """Test behaviour related to deleting old data when new overlapping data is ingested"""
    ingest_repetitions = 3
    input_dir = './tests/input_data/ingest/test_ingest/'
    expected_record_count = 20  # ten from each of two files

    product = GraceFOAcc1ADataProduct()
    version = DatasetVersion('04')
    instrument_id = 'C'

    def setUp(self):
        super().setUp()

        self.dataset = DatasetFactory.create(self.product, self.version, self.instrument_id)
        self.input_filepaths = sorted([
            f for f in glob.glob(os.path.join(self.input_dir, '*', '*'))
            if os.path.isfile(f)
        ])

    def test_repeated_ingestion_does_not_accumulate_data(self):

        previous_record_count = None

        for _ in range(self.ingest_repetitions):
            for fp in self.input_filepaths:
                ingest_file_to_db(self.product, fp)

            current_record_count = len(self.dataset.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                           datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                           aggregation_level=0,
                                                           limit_data_span=False))
            if previous_record_count is None:
                previous_record_count = current_record_count

            self.assertEqual(previous_record_count, current_record_count)

            previous_record_count = current_record_count

    def test_all_expected_data_present(self):

        for _ in range(self.ingest_repetitions):
            for fp in self.input_filepaths:
                ingest_file_to_db(self.product, fp)

            record_count = len(self.dataset.select(datetime(2000, 1, 1, tzinfo=timezone.utc),
                                                   datetime(2999, 1, 1, tzinfo=timezone.utc),
                                                   aggregation_level=0,
                                                   limit_data_span=False))

            self.assertEqual(self.expected_record_count, record_count)

    def test_validate_timestamp_primary(self):

        primary_datafile_correct = './tests/input_data/ingest/test_validate_timestamp/ACC1A_2023-06-03_C_04.txt'
        primary_datafile_incorrect = './tests/input_data/ingest/test_validate_timestamp/ACC1A_2023-06-04_C_04.txt'

        ingest_file_to_db(self.product, primary_datafile_correct) # all is good
        with self.assertRaises(TimestampOutliersException):
            ingest_file_to_db(self.product, primary_datafile_incorrect)

    def test_validate_timestamp_offred(self):

        class StubGraceFOOffredDataFileReader(GraceFOOffredDataFileReader):
            @classmethod
            def _get_current_input_file_column_def(cls, data_fpath: str):
                return super()._get_current_input_file_column_def(data_fpath, check_time_col_names=False)

        class StabGraceFOOffredDataProduct(GraceFOOffredDataProduct):
            @classmethod
            def get_reader(cls):
                return StubGraceFOOffredDataFileReader()

        os.environ['OFFRED_METADATA_FILEPATH'] = './tests/input_data/offred/fake_fields_metadata.json'
        offred_product = StabGraceFOOffredDataProduct()

        offred_datafile_correct = './tests/input_data/ingest/test_validate_timestamp/GF1_CX_12345_VVV_4_777777777_0001_22152062134_22152083936.out'
        offred_datafile_incorrect = './tests/input_data/ingest/test_validate_timestamp/GF1_CX_12345_VVV_4_777777777_0001_22153062134_22153083936.out'
        ingest_file_to_db(offred_product, offred_datafile_correct)  # all is good
        with self.assertRaises(TimestampOutliersException): # exception, time range in file name incorrect
            ingest_file_to_db(offred_product, offred_datafile_incorrect)  # all is good


if __name__ == '__main__':
    unittest.main()
