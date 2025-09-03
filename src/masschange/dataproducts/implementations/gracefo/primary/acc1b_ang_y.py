from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_ang_y import GraceFOAcc1bAngYDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOAcc1bAngYDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAcc1bAngYDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'ACC1B_ANG_Y'
    instrument_ids = {'C','D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time bigint not null,            
            y double precision not null,
            
            timestamp timestamptz not null 
        """
