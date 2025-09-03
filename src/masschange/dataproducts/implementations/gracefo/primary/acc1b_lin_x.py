from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_lin_x import GraceFOAcc1bLinXDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOAcc1bLinXDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAcc1bLinXDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'ACC1B_LIN_X'
    instrument_ids = {'C','D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time bigint not null,            
            x double precision not null,
            
            timestamp timestamptz not null 
        """
