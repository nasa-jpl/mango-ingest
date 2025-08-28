from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.clk1b import GraceFOClk1BDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOClk1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOClk1BDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'CLK1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=10)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcv_time bigint not null,
            GRACEFO_id CHAR not null,
            
            clock_id int not null,
            eps_time double precision not null,
            eps_err double precision not null,

            eps_drift double precision not null,
            drift_err double precision not null,
           
            qualflg VARCHAR(8) not null, 
            
            {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
