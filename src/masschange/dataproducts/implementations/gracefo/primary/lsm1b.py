from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.lsm1b import GraceFOLsm1BDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOLsm1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLsm1BDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'LSM1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1/10)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            GRACEFO_id CHAR not null,
            
            lof_yaw double precision not null,
            lof_pitch double precision not null,
            qualflg VARCHAR(8) not null,
            
            {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
