from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.qsa1b import GraceFOQsa1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOQsa1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOQsa1BDataFileReader()

    mission = GraceFO
    id_suffix = 'QSA1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=365) # TODO: Only one sample file is available, and this file has only one row. The time_series_interval is unknown.
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
        
                gps_time bigint not null,
                GRACEFO_id CHAR not null,
                
                sca_id int not null,
                quatangle double precision not null,
                quaticoeff double precision not null,
                quatjcoeff double precision not null,
                quatkcoeff double precision not null,
                qual_rss double precision not null,
              
                qualflg VARCHAR(8) not null, 
            
                timestamp timestamptz not null
        """
