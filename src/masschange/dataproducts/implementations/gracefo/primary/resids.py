from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.resids \
    import GraceFOResidsDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOResidsDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOResidsDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'RESIDS'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(seconds=5)
    processing_level = 'n/a'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time bigint not null,
            kbr_gps_residual double precision not null,
            
            timestamp timestamptz not null 
        """
