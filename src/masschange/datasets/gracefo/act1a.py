from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoact1a import GraceFOAct1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOAct1ADataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAct1ADataFileReader()

    mission = GraceFO
    id_suffix = 'ACT1A'
    stream_ids = {'C', 'D'}
    available_fields = {
        'rcvtime_intg',
        'rcvtime_frac',
        'GRACEFO_id',
        'qualflg',
        'lin_accl_x',
        'lin_accl_y',
        'lin_accl_z',
        'ang_accl_x',
        'ang_accl_y',
        'ang_accl_z',
        'icu_blk_nr',
        'timestamp'
    }

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            rcvtime_intg bigint not null,
            rcvtime_frac int not null,
            
            GRACEFO_id CHAR not null,
            qualflg VARCHAR(8) not null,
            
            lin_accl_x double precision not null,
            lin_accl_y double precision not null,
            lin_accl_z double precision not null,

            ang_accl_x double precision not null,
            ang_accl_y double precision not null,
            ang_accl_z double precision not null,

            icu_blk_nr int, 
            
            timestamp timestamptz not null
        """
