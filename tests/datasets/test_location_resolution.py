import logging
import unittest
from datetime import datetime, timezone

from masschange.dataproducts.implementations.gracefo.acc1a import GraceFOAcc1ADataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.dataproducts.timeseriesdatasetversion import TimeSeriesDatasetVersion

log = logging.getLogger()


class GeoResolutionTestCase(unittest.TestCase):
    """
    Tests that location is correctly resolved from timestamp.
    Requires connection to database with GNV1A data ingested
    """
    def test_correct_location_data_produced(self):
        product = GraceFOAcc1ADataProduct()
        dataset = TimeSeriesDataset(product, TimeSeriesDatasetVersion('04'), 'C')
        data_begin = datetime(2022, 6, 1, 0, tzinfo=timezone.utc)
        data_end = datetime(2022, 6, 1, 1, tzinfo=timezone.utc)
        dataset.select(data_begin, data_end, resolve_location=True)

