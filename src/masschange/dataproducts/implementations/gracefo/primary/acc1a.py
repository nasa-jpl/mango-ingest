from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1a import GraceFOAcc1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOAcc1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAcc1ADataFileReader()

    mission = GraceFO
    id_suffix = 'ACC1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(milliseconds=100)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            
            GRACEFO_id CHAR not null,
            qualflg VARCHAR(8) not null,
            
            lin_accl_x double precision not null,
            lin_accl_y double precision not null,
            lin_accl_z double precision not null,

            ang_accl_x double precision not null,
            ang_accl_y double precision not null,
            ang_accl_z double precision not null,
             
            icu_blk_nr int, 

            timestamp timestamptz not null
        """
