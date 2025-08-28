from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.imu1a import GraceFOImu1ADataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOImu1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOImu1ADataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'IMU1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1/8) # 8Hz, three gyros
    processing_level = '1A'

    # See comment in parent class - 8Hz is incompatible with global default aligned_bucket_span of 0.1Hz
    aligned_bucket_span: timedelta = time_series_interval * TimeSeriesDataProduct.aggregation_step_factor

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            
            GRACEFO_id CHAR not null,
            gyro_id smallint not null,
            FiltAng double precision not null,
            qualflg VARCHAR(8) not null,
            
            {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
