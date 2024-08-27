from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.clk1a import GraceFOClk1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOClk1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOClk1ADataFileReader()

    mission = GraceFO
    id_suffix = 'CLK1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=10)
    processing_level = '1A'

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
            
            timestamp timestamptz not null
        """
