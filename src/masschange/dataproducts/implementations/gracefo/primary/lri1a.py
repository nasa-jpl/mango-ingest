from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.lri1a import GraceFOLri1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOLri1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLri1ADataFileReader()

    mission = GraceFO
    id_suffix = 'LRI1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1/10)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            
            GRACEFO_id CHAR not null,
            
            prod_flag VARCHAR(16) not null,
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null,
            
            piston_phase double precision,
            phase0_int bigint,
            phase0_frac bigint,
            phase1_int bigint,
            phase1_frac bigint,
            phase2_int bigint,
            phase2_frac bigint,
            phase3_int bigint,
            phase3_frac bigint,
             tSnr int,
            noise8_9 int,
            noise11_12 int
        """
