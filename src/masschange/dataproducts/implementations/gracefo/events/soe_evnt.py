from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.events.soe_evnt import GraceFOSoeEventsDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.dataproduct import DataProduct


class GraceFOSoeEventsDataProduct(DataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOSoeEventsDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'SOE_EVNT'
    instrument_ids = {'C', 'D'}
    processing_level = None  # Events does not have a processing level.

    @classmethod
    def get_sql_table_schema(cls) -> str:
        return f"""
            soe_event VARCHAR(10),
            time VARCHAR(50),
            gps_time double precision not null,
            created VARCHAR(100),
            createdby VARCHAR(100),
            comments VARCHAR(1024),
            spacecraft VARCHAR(6),
            meta VARCHAR(500),
            timestamp timestamptz not null
        """
