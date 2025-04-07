from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.vgb1b import GraceFOVgb1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.dataproduct import DataProduct


class GraceFOVgb1BDataProduct(DataProduct):
    # TODO: Only one file is available. The sample file has 2 rows with the same time.
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOVgb1BDataFileReader()

    mission = GraceFO
    id_suffix = 'VGB1B'
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
            
            timestamp timestamptz not null
        """
