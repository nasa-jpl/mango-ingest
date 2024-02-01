from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoihk1a import GraceFOIhk1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOIhk1ADataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOIhk1ADataFileReader()

    mission = GraceFO
    id_suffix = 'IHK1A'
    stream_ids = {'C', 'D'}
    available_fields = {
        'GRACEFO_id',
        'qualflg',
        'sensortype',
        'sensorvalue',
        'sensorname',
        'rcvtime',
        'timestamp'
    }

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            GRACEFO_id CHAR not null,
            qualflg VARCHAR(8) not null,
            
            sensortype CHAR not null,
            sensorvalue double precision not null,
            sensorname VARCHAR(2) not null,

            rcvtime bigint not null,
            timestamp timestamptz not null
        """
