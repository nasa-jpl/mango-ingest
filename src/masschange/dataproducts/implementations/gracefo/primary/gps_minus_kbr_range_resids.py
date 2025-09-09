from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.gps_minus_kbr_range_resids \
    import GraceFOGpsMinusKbrRangeResidsDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOGpsMinusKbrRangeResidsDdDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGpsMinusKbrRangeResidsDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'GPS_MINUS_KBR_RANGE_RESIDS'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(seconds=5)
    processing_level = 'n/a'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time bigint not null,
            kbr_gps_residual double precision not null,
            
            timestamp timestamptz not null 
        """
