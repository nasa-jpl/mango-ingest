from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.gps1b import GraceFOGps1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOGps1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGps1BDataFileReader()

    mission = GraceFO
    id_suffix = 'GPS1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            
            GRACEFO_id CHAR not null,
            
            prn_id int not null,
            ant_id int not null,
            
            prod_flag VARCHAR(16) not null,
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null,
            
            CA_range double precision,
            L1_range double precision,
            L2_range double precision,
            CA_phase double precision,
            L1_phase double precision,
            L2_phase double precision,
            CA_SNR int,
            L1_SNR int,
            L2_SNR int,
            CA_chan int,
            L1_chan int,
            L2_chan int,
            L2_raw double precision,
            Ka_phase double precision,
            K_SNR double precision,
            Ka_SNR double precision  
        """
