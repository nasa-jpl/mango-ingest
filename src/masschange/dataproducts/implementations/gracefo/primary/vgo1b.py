from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.vgo1b import GraceFOVgo1BDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOVgo1BDataProduct(DataProduct):
    # TODO: Only one file is available. The sample file has 2 rows with the same time.
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOVgo1BDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'VGO1B'
    instrument_ids = {'C', 'D'}
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            gps_time bigint not null,
            
            GRACEFO_id CHAR not null,
            
            mag double precision not null,
            cosx double precision not null,
            cosy double precision not null,
            cosz double precision not null,
            
            qualflg VARCHAR(8) not null,
            
            {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
