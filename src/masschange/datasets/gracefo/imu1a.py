from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoimu1a import GraceFOImu1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOImu1ADataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOImu1ADataFileReader()

    mission = GraceFO
    id_suffix = 'IMU1A'
    stream_ids = {'C', 'D'}
    available_fields = {
        'GRACEFO_id',
        'gyro_id',
        'FiltAng',
        'qualflg'
        'rcvtime',
        'timestamp'
    }

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            GRACEFO_id CHAR not null,
            gyro_id smallint not null,
            FiltAng double precision not null,
            qualflg VARCHAR(8) not null,
            
            rcvtime bigint not null,
            timestamp timestamptz not null
        """
