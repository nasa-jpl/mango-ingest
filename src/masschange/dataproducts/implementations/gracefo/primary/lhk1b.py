from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.lhk1b import GraceFOLhk1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOLhk1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLhk1BDataFileReader()

    mission = GraceFO
    id_suffix = 'LHK1B'
    instrument_ids = {'C', 'D'}
    # TODO: This is not a time-series dataset. It has data from many different sensors,
    # reported at different rates, from 1 sec to 10 sec
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
