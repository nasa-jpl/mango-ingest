from datetime import timedelta

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.missions import Missions


class GraceFOOffredDataProduct(TimeSeriesDataProduct):

    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOOffredDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'OFFRED'
    instrument_ids = {'GF1', 'GF2'}
    # TODO: frequency is different per type of data. Need to figure out optimal common frequency
    time_series_interval = timedelta(seconds=30)
    processing_level = '0' # TODO: confirm it. May be it is 'None'

    # testing hypothesis that aligned_bucket_span < time_series_interval is causing performance issues
    # aligned_bucket_span = timedelta(minutes=10)

    def get_chunk_time_interval(cls) -> timedelta:
        return timedelta(hours=3)

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            utc VARCHAR(21) not null,  
            obt_integer bigint not null, 
            obt_fraction int not null, 
            obt_type VARCHAR(3),
            source_file_name VARCHAR(100),
            
            pcf_name VARCHAR(15),
            unit VARCHAR(15),
            value_int bigint ,
            value_float float,
            value_str VARCHAR(15),
            
            
            timestamp timestamptz not null 
        """
