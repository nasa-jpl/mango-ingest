from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.tim1a import GraceFOTim1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOTim1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOTim1ADataFileReader()

    mission = GraceFO
    id_suffix = 'TIM1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=8)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            obctime bigint not null,
            GRACEFO_id CHAR not null,
            TS_suppid int not null,
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            first_icu_blknr int not null,
            final_icu_blknr int not null,
        
            qualflg VARCHAR(8) not null, 
            
            timestamp timestamptz not null
        """
