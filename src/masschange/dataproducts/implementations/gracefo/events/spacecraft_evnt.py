from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.events.spacecraft_evnt import GraceFOSpacecraftEventsDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.dataproduct import DataProduct


class GraceFOSpacecraftEventsDataProduct(DataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOSpacecraftEventsDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'SPACECRAFT_EVNT'
    instrument_ids = {'C', 'D'}
    processing_level = None  # Events does not have a processing level.

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            spacecraft_event VARCHAR(100),
            time VARCHAR(50),
            gps_time double precision not null,
            created VARCHAR(100),
            createdby VARCHAR(100),
            spacecraft VARCHAR(6),
            data VARCHAR(10000),
            timestamp timestamptz not null
        """
