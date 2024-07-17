from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.gracefokbr1a import GraceFOKbr1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOKbr1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOKbr1ADataFileReader()

    mission = GraceFO
    id_suffix = 'KBR1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(milliseconds=100)
    processing_level = '1A'

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
            
            CA_SNR double precision,
            L1_SNR double precision,
            L2_SNR double precision,
            
            CA_chan int,
            L1_chan int,
            L2_chan int,
            
            K_phase double precision,
            Ka_phase double precision,
            
            K_SNR int,
            Ka_SNR int
        """
