from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoihk1a import GraceFOIhk1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOIhk1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOIhk1ADataFileReader()

    mission = GraceFO
    id_suffix = 'IHK1A'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=60)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            
            GRACEFO_id CHAR not null,
            qualflg VARCHAR(8) not null,
            
            sensortype CHAR not null,
            sensorvalue double precision not null,
            sensorname VARCHAR(2) not null,

            timestamp timestamptz not null
        """
