from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefomag1a import GraceFOMag1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOMag1ADataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOMag1ADataFileReader()

    mission = GraceFO
    id_suffix = 'MAG1A'
    stream_ids = {'C', 'D'}
    available_fields = {
        'GRACEFO_id',
        'MfvX_RAW',
        'MfvY_RAW',
        'MfvZ_RAW',
        'torque1A',
        'torque2A',
        'torque3A',
        'torque1B',
        'torque2B',
        'torque3B',
        'MF_BCalX',
        'MF_BCalY',
        'MF_BCalZ',
        'torque_cal',
        'qualflg',
        'rcvtime',
        'timestamp'
    }

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        return f"""
            GRACEFO_id CHAR not null,
            MfvX_RAW double precision not null,
            MfvY_RAW double precision not null,
            MfvZ_RAW double precision not null,
            torque1A double precision not null,
            torque2A double precision not null,
            torque3A double precision not null,
            torque1B double precision not null,
            torque2B double precision not null,
            torque3B double precision not null,
            MF_BCalX double precision not null,
            MF_BCalY double precision not null,
            MF_BCalZ double precision not null,
            torque_cal double precision not null,
            qualflg VARCHAR(8) not null,
            
            rcvtime bigint not null,
            timestamp timestamptz not null
        """
