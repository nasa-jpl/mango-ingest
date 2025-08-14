from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.tdp1b import GraceFOTdp1BDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct

class GraceFOTdp1BDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOTdp1BDataFileReader()

    mission = GraceFO
    id_suffix = 'TDP1B'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time double precision not null,
            nominal_value double precision not null,
            value double precision not null,
            sigma double precision not null,
            name VARCHAR(512) not null,
            timestamp timestamptz not null 
        """
