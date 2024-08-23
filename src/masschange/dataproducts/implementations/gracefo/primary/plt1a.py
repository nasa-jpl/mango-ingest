from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.plt1a import GraceFOPlt1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOPlt1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOPlt1ADataFileReader()

    mission = GraceFO
    id_suffix = 'PLT1A'
    instrument_ids = {'Y'}
    time_series_interval = timedelta(seconds=1)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            gps_time bigint not null,
            rcv_id CHAR not null,
            trx_id CHAR not null,
            tau double precision not null,

            xpos  double precision not null,
            ypos  double precision not null,
            zpos  double precision not null,

            xvel  double precision not null,
            yvel  double precision not null,
            zvel  double precision not null,

            qualflg VARCHAR(8) not null,
          
            timestamp timestamptz not null
        """
    
