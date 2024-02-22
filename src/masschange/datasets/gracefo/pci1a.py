from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefopci1a import GraceFOPci1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOPci1ADataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOPci1ADataFileReader()

    mission = GraceFO
    id_suffix = 'PCI1A'
    stream_ids = {'C', 'D'}

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        return f"""
            gps_time bigint not null,
            GRACEFO_id CHAR not null,
           
            ant_centr_corr double precision not null,
            ant_centr_rate double precision not null,
            ant_centr_accl double precision not null,
            
            qualflg VARCHAR(8) not null,
            
            timestamp timestamptz not null
        """
