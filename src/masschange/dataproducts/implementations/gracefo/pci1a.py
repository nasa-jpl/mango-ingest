from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefopci1a import GraceFOPci1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOPci1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOPci1ADataFileReader()

    mission = GraceFO
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
            
            timestamp timestamptz not null
        """
