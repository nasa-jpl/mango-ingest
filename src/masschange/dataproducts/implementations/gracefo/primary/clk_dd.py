from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.clk_dd import GraceFOClkDdDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOClkDdDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOClkDdDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'CLK_DD'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(seconds=10)
    processing_level = 'n/a'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time_c_prev bigint not null,
            clk_c_prev double precision not null,
            
            time_c_curr bigint not null,
            clk_c_curr double precision not null,
            
            time_d_prev bigint not null,
            clk_d_prev double precision not null,
            
            time_d_curr bigint not null,
            clk_d_curr double precision not null,
            
            clc_diff_c double precision not null,
            clc_diff_d double precision not null,
            clc_dd double precision not null,
            
            timestamp timestamptz not null 
        """
