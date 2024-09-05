from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.vgo1b import GraceFOVgo1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOVgo1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOVgo1BDataFileReader()

    mission = GraceFO
    id_suffix = 'VGO1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=365)  # TODO: Not a time series dataset. The sample file has 2 rows with the same time.
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
