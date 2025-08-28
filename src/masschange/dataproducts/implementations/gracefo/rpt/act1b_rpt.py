from datetime import timedelta

from masschange.ingest.executor.datafilereaders.base import DataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.rpt.act1b_rpt import GraceFOAct1BRptDataFileReader
from masschange.missions import Missions
from masschange.dataproducts.timeseriesrptdataproduct import TimeSeriesRptDataProduct


class GraceFOAct1BRptDataProduct(TimeSeriesRptDataProduct):
    @classmethod
    def get_reader(cls) -> DataFileReader:
        return GraceFOAct1BRptDataFileReader()

    mission = Missions.GraceFO
    id_suffix = 'ACT1B_RPT'
    instrument_ids = {'C', 'D'}
    time_series_interval = timedelta(days=1)
    processing_level = '1B'

    @classmethod
    def get_sql_table_schema(cls):
        additional_columns_schema = '''
            nr_no_data_gap_fill int not null,
            
            crms_lin_accl_x double precision not null, 
            crms_lin_accl_y double precision not null, 
            crms_lin_accl_z	double precision not null, 
            
            crms_ang_accl_x	double precision not null, 
            crms_ang_accl_y	double precision not null, 
            crms_ang_accl_z	double precision not null, 
            
            rel_bias_x double precision not null, 
            rel_bias_y double precision not null, 
            rel_bias_z double precision not null, 
            
            rel_scale_x double precision not null, 
            rel_scale_y double precision not null, 
            rel_scale_z double precision not null, 
            
            rel_res_x double precision not null, 
            rel_res_y double precision not null, 
            rel_res_z double precision not null,
        '''
        return cls.insert_additional_columns_schema(super().get_sql_table_schema(),
                                                    additional_columns_schema)
