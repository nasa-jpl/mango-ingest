from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.icsnr import GraceFOIcsnrDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOIcsnrDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOIcsnrDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'ICSNR'
    instrument_ids = {'C','D'}
    time_series_interval = timedelta(seconds=10)
    processing_level = '1A' # TODO: verify. Level is not in the file name

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time double precision not null,
            k_minus_0_75ka double precision not null,
            k_snr double precision not null,
            ka_snr double precision not null,
            subset_version int not null,
            timestamp timestamptz not null 
        """
