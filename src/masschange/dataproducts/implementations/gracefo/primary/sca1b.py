from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.sca1b import GraceFOSca1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOSca1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOSca1BDataFileReader()

    mission = GraceFO
    id_suffix = 'SCA1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            gps_time bigint not null,
            GRACEFO_id CHAR not null,
            
            sca_id smallint not null,
            quatangle double precision not null,
            quaticoeff double precision not null,
            quatjcoeff double precision not null,
            quatkcoeff double precision not null,
            qual_rss double precision not null,
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null
        """
