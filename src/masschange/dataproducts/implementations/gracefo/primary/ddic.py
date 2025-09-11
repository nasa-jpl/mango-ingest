from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.ddic import GraceFODdicDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFODdicDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFODdicDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'DDIC'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(seconds=10)
    processing_level = '1A' # TODO: verify. Level is not in the file name

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time double precision not null,
            ddic double precision not null,
            
            timestamp timestamptz not null 
        """
