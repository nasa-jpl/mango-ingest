from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoclk1b import GraceFOClk1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOClk1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOClk1BDataFileReader()

    mission = GraceFO
    id_suffix = 'CLK1B'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
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
