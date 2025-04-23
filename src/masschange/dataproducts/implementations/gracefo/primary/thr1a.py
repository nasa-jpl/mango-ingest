from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.thr1a import GraceFOThr1ADataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.utils import get_schema_updates_for_flag_fields


class GraceFOThr1ADataProduct(DataProduct):
    """
    This is not a time-series dataproduct; measurement intervals are irregular
    """
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOThr1ADataFileReader()

    mission = GraceFO
    id_suffix = 'THR1A'
    instrument_ids = {'C', 'D'}
    processing_level = '1A'

    @classmethod
    def get_sql_table_schema(cls) -> str:
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
            {get_schema_updates_for_flag_fields("qualflg", 8)}
            
            timestamp timestamptz not null 
        """
