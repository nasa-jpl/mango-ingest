from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.offred.offred_num import GraceFOOffredNumDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOOffredNumDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOOffredNumDataFileReader()

    mission = GraceFO
    id_suffix = 'OFFRED_NUM'
    instrument_ids = {'GF1', 'GF2'}
    time_series_interval = timedelta(milliseconds=100)
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
            
            pcf_name VARCHAR(15),
            unit VARCHAR(15),
            field_value double precision not null,
            
            timestamp timestamptz not null 
        """
