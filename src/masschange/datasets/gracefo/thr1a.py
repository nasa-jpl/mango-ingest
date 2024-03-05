from datetime import timedelta

from masschange.ingest.datafilereaders.base import DataFileReader
from masschange.ingest.datafilereaders.gracefothr1a import GraceFOThr1ADataFileReader
from masschange.missions import GraceFO
from masschange.datasets.timeseriesdataset import TimeSeriesDataset


class GraceFOThr1ADataset(TimeSeriesDataset):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOThr1ADataFileReader()

    mission = GraceFO
    id_suffix = 'THR1A'
    stream_ids = {'C', 'D'}
    time_series_interval = timedelta(seconds=120)  # TODO: THR1A is not a time-series dataset, and measurement intervals are irregular.  Once non-timeseries dataset classes are implemented, this should be switched to the appropriate base class

    @classmethod
    def _get_sql_table_schema(cls) -> str:
        # NOTE: qualflag bit 7 = No ICU block number available for GRACE-FO,
        # so assume that icu_blk_nr could be NULL
        return f"""
            time_intg bigint not null,
            time_frac int not null,
            GRACEFO_id CHAR not null,
            
            thrust_count_att_ctrl_1_1 int not null,
            thrust_count_att_ctrl_1_2 int not null,
            thrust_count_att_ctrl_1_3 int not null,
            thrust_count_att_ctrl_1_4 int not null,
            thrust_count_att_ctrl_1_5 int not null,
            thrust_count_att_ctrl_1_6 int not null,
            thrust_count_att_ctrl_2_1 int not null,
            thrust_count_att_ctrl_2_2 int not null,
            thrust_count_att_ctrl_2_3 int not null,
            thrust_count_att_ctrl_2_4 int not null,
            thrust_count_att_ctrl_2_5 int not null,
            thrust_count_att_ctrl_2_6 int not null,
    
            on_time_att_ctrl_1_1 int not null,
            on_time_att_ctrl_1_2 int not null,
            on_time_att_ctrl_1_3 int not null,
            on_time_att_ctrl_1_4 int not null,
            on_time_att_ctrl_1_5 int not null,
            on_time_att_ctrl_1_6 int not null,
            on_time_att_ctrl_2_1 int not null,
            on_time_att_ctrl_2_2 int not null,
            on_time_att_ctrl_2_3 int not null,
            on_time_att_ctrl_2_4 int not null,
            on_time_att_ctrl_2_5 int not null,
            on_time_att_ctrl_2_6 int not null,
            on_time_orb_ctrl_1 int not null,
            on_time_orb_ctrl_2 int not null,

            accum_dur_att_ctrl int not null,   
            accum_dur_orb_ctrl int not null, 

            qualflg VARCHAR(8) not null,
            timestamp timestamptz not null
        """
