from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.ac01a_rpt import GraceFOAc01ARptDataFileReader
from masschange.missions import GraceFO
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOAc01ARptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAc01ARptDataFileReader()

    mission = GraceFO
    id_suffix = 'AC01A_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls):
        additional_columns_schema = '''
        
            nrec_read int not null,
            nrec_read_used int not null,
            nrec_written int not null,
            nrec_nulled int not null,
            nrec_non_incorporated int not null,
            nrec_filled int not null,
            nrec_consistency int not null,
            
            n_yaw_minus_thrust int not null,
            min_yaw_minus_thrust_dur int not null,
            max_yaw_minus_thrust_dur int not null,
        
            n_pitch_p_thrust int not null,
            max_pitch_p_thrust_dur int not null,
            min_pitch_p_thrust_dur int not null,
        
            n_yaw_p_thrust int not null,
            max_yaw_p_thrust_dur int not null,
            min_yaw_p_thrust_dur int not null,
        
            n_pitch_m_thrust int not null,
            max_pitch_m_thrust_dur int not null,
            min_pitch_m_thrust_dur int not null,
        
            n_roll_m_thrust int not null,
            max_roll_m_thrust_dur int not null,
            min_roll_m_thrust_dur int not null,
        
            n_roll_p_thrust int not null,
            max_roll_p_thrust_dur int not null,
            min_roll_p_thrust_dur int not null,
        
            n_oct1_thrust int not null,
            max_oct1_thrust_dur int not null,
            min_oct1_thrust_dur int not null,
        
            n_oct2_thrust int not null,
            max_oct2_thrust_dur int not null,
            min_oct2_thrust_dur int not null,

        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
