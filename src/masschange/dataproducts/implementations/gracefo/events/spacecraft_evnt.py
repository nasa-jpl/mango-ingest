from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.events.spacecraft_events import GraceFOSpacecraftEventsDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.dataproduct import DataProduct


class GraceFOSpacecraftEventsDataProduct(DataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOSpacecraftEventsDataFileReader()

    mission = GraceFO
    id_suffix = 'SPACECRAFT_EVNT'
    instrument_ids = {'Y'}
    processing_level = None  # Events does not have a processing level.

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            spacecraftevent VARCHAR(100),
            time VARCHAR(50),
            created VARCHAR(50),
            createdby VARCHAR(50),
            spacecraft VARCHAR(6),
            meta VARCHAR(100),
            timestamp timestamptz not null
        """
