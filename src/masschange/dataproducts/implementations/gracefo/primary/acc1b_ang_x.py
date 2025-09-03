from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_ang_x import GraceFOAcc1bAngXDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOAcc1bAngXDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAcc1bAngXDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'ACC1B_ANG_X'
    instrument_ids = {'C','D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = 'n/a'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time bigint not null,            
            x double precision not null,
            
            timestamp timestamptz not null 
        """
