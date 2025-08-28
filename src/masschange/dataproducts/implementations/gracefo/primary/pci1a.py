from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.pci1a import GraceFOPci1ADataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOPci1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOPci1ADataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'PCI1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=5)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            gps_time bigint not null,
            GRACEFO_id CHAR not null,
           
            ant_centr_corr double precision not null,
            ant_centr_rate double precision not null,
            ant_centr_accl double precision not null,
            
            qualflg VARCHAR(8) not null,
            
            {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
