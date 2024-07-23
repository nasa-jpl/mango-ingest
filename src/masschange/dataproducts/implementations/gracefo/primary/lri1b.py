from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.lri1b import GraceFOLri1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOLri1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLri1BDataFileReader()

    mission = GraceFO
    id_suffix = 'LRI1B'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(seconds=2)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            gps_time bigint not null,
            
            biased_range double precision not null,
            range_rate double precision not null,
            range_accl double precision not null,
            iono_corr double precision not null,
            lighttime_corr double precision not null,
            lighttime_rate double precision not null,
            lighttime_accl double precision not null,
            K_A_SNR double precision not null,  
            K_B_SNR double precision not null,
            
            qualflg VARCHAR(8) not null,
            timestamp timestamptz not null
        """
