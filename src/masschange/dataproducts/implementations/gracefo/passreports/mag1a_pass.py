from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.passreports.mag1a_pass import GraceFOMag1APassDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOMag1APassDataProduct(TimeSeriesRptDataProduct):
    # TODO: sample files has extra columns that are not listed at the GMAT website
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOMag1APassDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'MAG1A_PASS'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(hours=3)
    processing_level = '1A'

