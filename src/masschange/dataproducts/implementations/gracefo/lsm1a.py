from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefolsm1a import GraceFOLsm1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOLsm1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLsm1ADataFileReader()

    mission = GraceFO
    id_suffix = 'LSM1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            
            GRACEFO_id CHAR not null,
            internalSensor0 int not null,
            internalSensor1 int not null,
            commanded0 int not null,
            commanded1 int not null,
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null
        """
