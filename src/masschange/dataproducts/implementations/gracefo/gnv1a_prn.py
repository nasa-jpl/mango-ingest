from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefognv1a_prn import GraceFOGnv1APrnDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOGnv1APrnDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOGnv1APrnDataFileReader()

    mission = GraceFO
    id_suffix = 'GNV1A_PRN'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=2)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcv_time bigint not null,
            n_prns int not null,
            GRACEFO_id CHAR not null,

            prn_id  double precision not null,
            el_prn  double precision not null,
            az_prn  double precision not null,

            timestamp timestamptz not null
        """
