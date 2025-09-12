from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.kbr1a_k_band import GraceFOKbr1AKBandDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct


class GraceFOKbr1AKBandDataProduct(TimeSeriesDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOKbr1AKBandDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'KBR1A_K_BAND'
    instrument_ids = {'C','D'}
    time_series_interval = timedelta(seconds=200)
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            time double precision not null,            
            k_band_freq double precision not null,
            
            timestamp timestamptz not null 
        """
