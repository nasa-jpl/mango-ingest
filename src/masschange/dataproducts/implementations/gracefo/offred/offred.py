from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred import GraceFOOffredDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOOffredDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOOffredDataFileReader()

    mission = GraceFO
    id_suffix = 'OFFRED'
    instrument_ids = {'GF1', 'GF2'}
    # TODO: frequency is different per type of data. Need to figure out optimal common frequency
    time_series_interval = timedelta(seconds=30)
    processing_level = '1A' # TODO: confirm it. May be it is 'None'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
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
            value_str VARCHAR(100),
            
            
            timestamp timestamptz not null 
        """
