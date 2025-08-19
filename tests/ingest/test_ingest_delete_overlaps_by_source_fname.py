import os
import unittest
from tests.ingest.base import IngestTestCaseBase
from datetime import datetime, timezone

from masschange.dataproducts.implementations.gracefo.primary.acc1a import GraceFOAcc1ADataProduct
from masschange.dataproducts.implementations.gracefo.offred.offred import GraceFOOffredDataProduct
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.ingest.executor.ingest import ingest_file_to_db
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.datasetfactory import DatasetFactory


class StubGraceFOOffredDataFileReader(GraceFOOffredDataFileReader):

    @classmethod
    def _get_current_input_file_column_def(cls, data_fpath: str):
        return super()._get_current_input_file_column_def(data_fpath, check_time_col_names=False)

class StabGraceFOOffredDataProduct(GraceFOOffredDataProduct):
    @classmethod
    def get_reader(cls):
        return StubGraceFOOffredDataFileReader()

class DataOverwriteByFileNameIngestTestCase(IngestTestCaseBase):
    """Test behaviour related to deleting old data based on source file name when new overlapping data is ingested"""
    ingest_repetitions = 3
    product = StabGraceFOOffredDataProduct()
    version = DatasetVersion('04')
    data_file1 = './tests/input_data/offred/GF1_CX_777777_XXX_7_777777777_7777_77777777777_77777777777.out'
    data_file2 = './tests/input_data/GF1_CX_888888_XXX_7_777777777_7777_77777777777_77777777777.out'
    expected_record_count = 84 # 6 lines, 7 variables, 2 files
    def setUp(self):
        self.dataset = DatasetFactory.create(self.product, DatasetVersion('01'), 'GF1' )
        os.environ['OFFREAD_METADATA_FILE'] = './tests/input_data/offred/fake_fields_metadata.json'
        super().__init__()

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

    def test_throw_if_no_source_file_name_column(self):
            # test that an attempt to use data product with DELETE_OVERLAP_ON_INGEST_BASED_ON_SOURCE_FILE_NAME=True
            # and without SOURCE_FILE_COLUMN_NAME will throw
            bad_product = GraceFOAcc1ADataProduct()
            bad_product.DELETE_OVERLAP_ON_INGEST_BASED_ON_SOURCE_FILE_NAME = True
            with self.assertRaisesRegex(RuntimeError, "GraceFOAcc1ADataProduct does not have a column 'source_file_name'"):
                ingest_file_to_db(bad_product,  './tests/input_data/ingest/test_ingest/ACC1A_2023-06-03_C_04.txt')

if __name__ == '__main__':
    unittest.main()
