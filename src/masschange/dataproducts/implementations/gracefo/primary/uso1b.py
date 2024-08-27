from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.uso1b import GraceFOUso1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOUso1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOUso1BDataFileReader()

    mission = GraceFO
    id_suffix = 'USO1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)  # TODO: Uso is not a time-series dataset. It has a reference info that gets reported ones a day. Once non-timeseries dataset classes are implemented, this should be switched to the appropriate base class
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            gps_time bigint not null,
            
            GRACEFO_id CHAR not null,
           
            uso_id int not null,
            uso_freq double precision,
            K_freq double precision,
            Ka_freq double precision,
            qualflg VARCHAR(8) not null,
           
            timestamp timestamptz not null
              
        """
