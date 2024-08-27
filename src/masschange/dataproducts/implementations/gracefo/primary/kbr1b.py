from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.kbr1b import GraceFOKbr1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOKbr1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOKbr1BDataFileReader()

    mission = GraceFO
    id_suffix = 'KBR1B'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(seconds=5)
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
            ant_centr_corr double precision not null,
            ant_centr_rate double precision not null,
            ant_centr_accl double precision not null,
            K_A_SNR double precision not null, 
            Ka_A_SNR double precision not null,
            K_B_SNR double precision not null,
            Ka_B_SNR double precision not null,
            
            qualflg VARCHAR(8) not null,
            timestamp timestamptz not null
        """
