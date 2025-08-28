from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.passreports.gps1a_pass import GraceFOGps1APassDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOGps1APassDataProduct(TimeSeriesRptDataProduct):
    # Note: sample files has extra columns that are not listed at the GMAT website.
    # According to Chris, the extra columns could be ignored for Grace FO
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGps1APassDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'GPS1A_PASS'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(hours=3)
    processing_level = '1A'

