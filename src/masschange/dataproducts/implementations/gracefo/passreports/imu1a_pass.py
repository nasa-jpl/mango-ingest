from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.passreports.imu1a_pass import GraceFOImu1APassDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOImu1APassDataProduct(TimeSeriesRptDataProduct):
    # TODO: sample files has extra columns that are not listed at the GMAT website
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOImu1APassDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'IMU1A_PASS'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(hours=3)
    processing_level = '1A'

