"""Test behaviour related to updating metadata upon ingest"""

import os
import unittest

from masschange.dataproducts.implementations.gracefo.primary.gps1a import GraceFOGps1ADataProduct
from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.ingest.executor.ingest import ingest_file_to_db
from tests.ingest.base import IngestTestCaseBase


class IngestChannelIdMetadataUpdateTestCase(IngestTestCaseBase):
    input_dir = './tests/input_data/ingest/test_metadataupdates/'

    product = GraceFOGps1ADataProduct()
    version = DatasetVersion('04')
    instrument_id = 'C'

    def setUp(self):
        self.dataset = DatasetFactory.create(self.product, self.version, self.instrument_id)
        self.input_filepaths = [os.path.join(self.input_dir, fn) for fn in os.listdir(self.input_dir)]

        super().__init__()

    def test_channel_id_metadata_updated_after_ingest(self):
        # Currently does not test that metadata is appended rather than replaced, but this can be reasonably inferred
        # from the fact that rows are never deleted from SQL until such a test is implemented

        expected_empty_metadata = {'ant_id': [], 'prn_id': []}
        actual_empty_metadata = {field.name: [str(v) for v in values] for field, values in
                           self.dataset.fetch_channel_id_values().items()}

        self.assertDictEqual(expected_empty_metadata, actual_empty_metadata,
                             'Channel-id metadata should be empty prior to ingestion')

        for fp in self.input_filepaths:
            ingest_file_to_db(self.product, fp)

        expected_metadata = {'ant_id': ['0'], 'prn_id': ['11', '12', '19', '25', '28', '29', '31', '32', '6']}
        actual_metadata = {field.name: [str(v) for v in values] for field, values in
                           self.dataset.fetch_channel_id_values().items()}
        self.assertDictEqual(expected_metadata, actual_metadata)


if __name__ == '__main__':
    unittest.main()
