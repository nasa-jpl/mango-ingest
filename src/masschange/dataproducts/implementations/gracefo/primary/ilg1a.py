from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefo.primary.ilg1a import GraceFOIlg1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOIlg1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOIlg1ADataFileReader()

    mission = GraceFO
    id_suffix = 'ILG1A'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(hours=1)  # TODO: This is not a time-series dataset.
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            rcv_time bigint not null,
            pkt_count int not null,
            
            GRACEFO_id CHAR not null,
            logpacket VARCHAR(1000),
            timestamp timestamptz not null
        """
