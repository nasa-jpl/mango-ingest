from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefoact1b import GraceFOAct1BDataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOAct1BDataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAct1BDataFileReader()

    mission = GraceFO
    id_suffix = 'ACT1B'
    stream_ids = {'C', 'D'}
    available_fields = {
        'gps_time',
        'GRACEFO_id',

        'lin_accl_x',
        'lin_accl_y',
        'lin_accl_z',
        'acl_x_res',
        'acl_y_res',
        'acl_z_res',
        'qualflg',
        'timestamp'
    }

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            gps_time bigint not null,
            GRACEFO_id CHAR not null,
            
            lin_accl_x double precision not null,
            lin_accl_y double precision not null,
            lin_accl_z double precision not null,

            acl_x_res double precision not null,
            acl_y_res double precision not null,
            acl_z_res double precision not null,

            qualflg VARCHAR(8) not null, 
            
            timestamp timestamptz not null
        """
