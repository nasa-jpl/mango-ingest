from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.tdp1a import GraceFOTdp1ADataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOTdp1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOTdp1ADataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'TDP1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time bigint not null,
            nominal_value double precision not null,
            value double precision not null,
            sigma double precision not null,
            name VARCHAR(512) not null,
            timestamp timestamptz not null 
        """
