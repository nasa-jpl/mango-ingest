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
            time bigint not null,
            
            clc_diff_c double precision not null,
            clc_diff_d double precision not null,
            clc_dd double precision not null,
            
            timestamp timestamptz not null 
        """
