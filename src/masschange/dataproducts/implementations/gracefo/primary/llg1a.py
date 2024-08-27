from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.llg1a import GraceFOLlg1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOLlg1ADataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOLlg1ADataFileReader()

    mission = GraceFO
    id_suffix = 'LLG1A'
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
