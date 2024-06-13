from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoimu1b import GraceFOImu1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOImu1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOImu1BDataFileReader()

    mission = GraceFO
    id_suffix = 'IMU1B'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1/8) # 8Hz, three gyros
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            
            GRACEFO_id CHAR not null,
            gyro_id smallint not null,
            FiltAng double precision not null,
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null
        """
