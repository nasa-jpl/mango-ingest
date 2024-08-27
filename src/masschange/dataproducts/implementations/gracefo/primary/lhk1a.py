from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.lhk1a import GraceFOLhk1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOLhk1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLhk1ADataFileReader()

    mission = GraceFO
    id_suffix = 'LHK1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        # TODO: according to the description in the fle header,
        #  max number of characters in sensorname is 1000.
        #  Actual names seems much smaller. Consider reducing the VARCHAR size.
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            GRACEFO_id CHAR not null,
            qualflg VARCHAR(8) not null,
            
            sensortype CHAR not null,
            sensorvalue bigint not null,
            sensorname VARCHAR(1000) not null,
            
            timestamp timestamptz not null
        """
