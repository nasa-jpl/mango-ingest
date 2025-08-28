from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.qcp1b import GraceFOQcp1BDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOQcp1BDataProduct(DataProduct):
    # TODO: Only one sample file is available, and this file has only one row. The time_series_interval is unknown.
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOQcp1BDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'QCP1B'
    instrument_ids = {'C', 'D'}
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
        
                gps_time bigint not null,
                GRACEFO_id CHAR not null,

                quatangle double precision not null,
                quaticoeff double precision not null,
                quatjcoeff double precision not null,
                quatkcoeff double precision not null,
                qual_rss double precision not null,
              
                qualflg VARCHAR(8) not null, 
            
                {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
